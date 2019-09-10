package com.huawei.hwcloud.tarus.kvstore.service.race;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicInteger;

import com.firenio.common.ByteUtil;
import com.firenio.common.Unsafe;
import com.firenio.log.LoggerFactory;
import com.huawei.hwcloud.tarus.kvstore.common.KVStoreRace;
import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;

import static com.huawei.hwcloud.tarus.kvstore.service.race.ActionType.*;
import static com.huawei.hwcloud.tarus.kvstore.service.race.Env.*;
import static com.huawei.hwcloud.tarus.kvstore.service.race.Util.*;

public class KVService implements KVStoreRace {

    static final ThreadLocal<byte[]> READ_VAL_BUF = ThreadLocal.withInitial(KVService::newREAD_VAL_BUF);
    static final boolean             step1;
    static final boolean             step2;

    static {
        LoggerFactory.setEnableSLF4JLogger(false);
        String f1 = System.getProperty("kvs.clear.data");
        String f2 = System.getProperty("kvs.check.mode");
        step1 = "verify_perf0".equals(f2 + f1);
        step2 = "verify_perf1".equals(f2 + f1);
    }

    final LongIntMap        index_map  = new LongIntMap(DATA_COUNT);
    final byte[]            send_buf   = new byte[1024 * 8];
    final byte[]            read_buf   = new byte[DATA_1M];
    final LRUItem[]         ALL_CACHE  = new LRUItem[DATA_COUNT / DATA_COUNT_PER_BLOCK];
    final LRUItem[]         REAL_CACHE = new LRUItem[LRU_SIZE];
    final InetSocketAddress server_address;

    Socket       socket;
    OutputStream socketOutput;
    InputStream  socketInput;

    int           index;
    int           index_size;
    boolean       init;
    AtomicInteger get_log_count = new AtomicInteger();
    AtomicInteger set_log_count = new AtomicInteger();

    public KVService() {
        String host = get_server_host();
        this.server_address = new InetSocketAddress(host, PORT);
        for (int i = 0; i < LRU_SIZE; i++) {
            REAL_CACHE[i] = new LRUItem();
        }
    }

    static String get_server_host() {
        if (!ONLINE) {
            return "127.0.0.1";
        }
        String host = com.firenio.common.Util.getStringProperty("kvs.rpc.uri");
        log("server host:" + host);
        if (host == null) {
            throw new RuntimeException("get host error");
        }
        try {
            URI uri = new URI(host);
            return uri.getHost();
        } catch (URISyntaxException e) {
            printException(e);
            throw new RuntimeException(e);
        }
    }

    static byte[] newREAD_VAL_BUF() {
        return new byte[VALUE_LEN];
    }

    @Override
    public boolean init(final String dir, final int thread_num) throws KVSException {
        if (init) {
            log("already init");
            return true;
        }
        this.init = true;
        this.index = thread_num;
        log("init ..." + index);
        this.try_connect();
        write_action(ACTION_INIT);
        byte[] read_array = read_buf;
        int    read       = do_read(read_array, 8);
        if (read == -1) {
            log("init read error");
            return false;
        }
        int len = ByteUtil.getInt(read_array, 4);
        log("init len: " + len);
        LongIntMap index_map  = this.index_map;
        int        read1      = 0;
        int        block      = DATA_256K;
        int        count      = len / block;
        int        remain     = len % block;
        int        index_size = 0;
        for (int i = 0; i < count; i++) {
            read = do_read(read_array, block);
            if (read == -1) {
                log("init read error");
                return false;
            }
            int i_size = block / 8;
            for (int j = 0; j < i_size; j++) {
                index_map.put(ByteUtil.getLongLE(read_array, j << 3), index_size++);
            }
        }
        if (remain > 0) {
            read = do_read(read_array, remain);
            if (read == -1) {
                log("init read error");
                return false;
            }
            int i_size = remain / 8;
            for (int j = 0; j < i_size; j++) {
                index_map.put(ByteUtil.getLongLE(read_array, j << 3), index_size++);
            }
        }
        this.index_size = index_size;
        log("received server res: init finish: " + index_map.size() + ", inst_id: " + index);
        return true;
    }

    private void try_connect() {
        try {
            this.socket = new Socket();
            this.socket.setSoTimeout((int) TIMEOUT);
//            this.socket.setTcpNoDelay(true);
            this.socket.connect(server_address);
            this.socketInput = socket.getInputStream();
            this.socketOutput = socket.getOutputStream();
        } catch (Exception e) {
            printException(e);
            throw new RuntimeException(e);
        }
    }

    private void write_action(int action) {
        byte[] send_buf = this.send_buf;
        send_buf[0] = get_header(index, action);
        do_write(send_buf, 0, 1);
    }

    private int do_read(byte[] data, int len) {
        InputStream input = this.socketInput;
        int         read  = 0;
        try {
            for (; read < len; ) {
                int r = input.read(data, read, len - read);
                if (r == -1) {
                    return -1;
                }
                read += r;
            }
            return read;
        } catch (IOException e) {
            printException(e);
            return -1;
        }
    }

    private void do_write(byte[] data, int off, int len) {
        try {
            socketOutput.write(data, off, len);
        } catch (IOException e) {
            printException(e);
        }
    }

    private void read1byte() {
        byte[] read_buf = this.read_buf;
        try {
            socketInput.read(read_buf, 0, 1);
        } catch (IOException e) {
            printException(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public long set(final String key, final byte[] val) throws KVSException {
        long set_count = set_log_count.getAndIncrement();
        if (index == 0 && set_count < 10) {
            log("set key: " + key);
        }
        long long_key = string2long(key);
        this.index_map.put(long_key, index_size++);
        byte[] send_buf = this.send_buf;
        send_buf[0] = get_header(index, ACTION_SET);
        ByteUtil.putLongLE(send_buf, long_key, 4);
        System.arraycopy(val, 0, send_buf, 12, VALUE_LEN);
        do_write(send_buf, 0, VALUE_LEN + 12);
        read1byte();
        return -1;
    }

    public LRUItem free_last() {
        LRUItem item = REAL_CACHE[0];
        for (int i = 1; i < LRU_SIZE; i++) {
            LRUItem temp = REAL_CACHE[i];
            if (temp.last_access < item.last_access) {
                item = temp;
            }
        }
        if (item.index != -1) {
            ALL_CACHE[item.index] = null;
            item.index = -1;
        }
        return item;
    }

    @Override
    public long get(final String key, final Ref<byte[]> val) throws KVSException {
        long log_count = get_log_count.getAndIncrement();
        long long_key  = string2long(key);
        int  key_index = index_map.get(long_key);
        if (index == 0 && log_count < 10) {
            log("get key: " + key + ", index: " + key_index);
        }
        if (!ONLINE && log_count == 1000) {
            com.firenio.common.Util.exec(() -> {
                com.firenio.common.Util.sleep(1000);
                com.firenio.common.Util.close(socket);
            });
        }
        if (key_index == -1) {
            val.setValue(null);
            return 0;
        }
        int     cache_index = key_index / DATA_COUNT_PER_BLOCK;
        long    val_pos     = 1L * key_index * VALUE_LEN;
        LRUItem item        = ALL_CACHE[cache_index];
        if (item == null) {
            long read_pos = 1L * cache_index * READ_BLOCK_SIZE;
            if (log_count < 10 && index == 0) {
                log("cache miss, read data from server..., read_pos: " + read_pos);
            }
            item = free_last();
            byte[] read_buf = item.buf;
            byte[] send_buf = this.send_buf;
            send_buf[0] = get_header(index, ACTION_READ);
            ByteUtil.putLongLE(send_buf, read_pos, 4);
            do_write(send_buf, 0, 12);
            int read = do_read(read_buf, 8);
            if (read != -1) {
                int data_len = ByteUtil.getIntLE(read_buf, 4);
                read = do_read(read_buf, data_len);
            }
            if (read == -1) {
                try_connect();
                log("reconnect success, try get key: " + key);
                return get(key, val);
            }
            item.set_index(cache_index);
            ALL_CACHE[cache_index] = item;
        }
        if (index == 0 && log_count < 10) {
            log("read data from cache..., val_pos: " + val_pos);
        }
        byte[] res = READ_VAL_BUF.get();
        item.read(val_pos, res);
        val.setValue(res);
        return 0;
    }

    @Override
    public void close() {
        if (init) {
            init = false;
            log("close ..." + index);
            write_action(ACTION_CLOSE);
            read1byte();
            log("close finish..." + index);
            com.firenio.common.Util.close(this.socket);
        }
    }

    @Override
    public void flush() {
        log("flush ..." + index);
        write_action(ACTION_FLUSH);
        read1byte();
        log("flush finish..." + index);
    }

    static final class LRUItem {

        final byte[] buf = new byte[READ_BLOCK_SIZE];
        long last_access = 0;
        int  index       = -1;
        long start_pos;

        void set_index(int index) {
            this.index = index;
            this.start_pos = 1L * index * READ_BLOCK_SIZE;
        }

        public void read(long pos, byte[] res) {
            this.last_access = System.currentTimeMillis();
            System.arraycopy(buf, (int) (pos - start_pos), res, 0, VALUE_LEN);
        }
    }

}
