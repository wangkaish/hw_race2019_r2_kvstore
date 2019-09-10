package com.huawei.hwcloud.tarus.kvstore.store.race;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;

import moe.cnkirito.kdio.DirectIOLib;
import moe.cnkirito.kdio.DirectIOUtils;
import moe.cnkirito.kdio.DirectRandomAccessFile;

import com.firenio.buffer.ByteBuf;
import com.firenio.common.Unsafe;
import com.firenio.common.Util;
import com.firenio.component.Channel;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;
import com.huawei.hwcloud.tarus.kvstore.service.race.LongIntMap;

import static com.huawei.hwcloud.tarus.kvstore.service.race.Env.*;
import static com.huawei.hwcloud.tarus.kvstore.service.race.Util.*;

public class EngineKVStoreRace {

    static final byte         UN_KEY        = -1;
    static final int          TEMP_FILE_LEN = ONLINE ? 256 : 3;
    static final OpenOption[] FC_OPS        = new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE};

    FileChannel      data_channel;
    FileChannel      index_channel;
    FileChannel      temp_channel;
    DioReader        data_reader;
    DioReader        data_writer;
    MappedByteBuffer index_buf;
    MappedByteBuffer temp_buf;
    long             temp_buf_addr;
    long             data_write_cost = 0;
    boolean          p_init          = false;
    int              inst_id;

    static void print_index_map(LongIntMap map) {
        for (map.scan(); map.hasNext(); ) {
            log("key: " + map.key() + ", pos: " + map.value());
        }
    }

    static ByteBuffer new_read_buf() {
        return ByteBuffer.allocate(VALUE_LEN);
    }

    public boolean init(final int inst_id) throws KVSException {
        if (p_init) {
            log("already init");
            return true;
        }
        this.p_init = true;
        this.inst_id = inst_id;
        log("file_size: " + inst_id);
        String root_path = PATH;
        File   root      = new File(PATH);
        if (!root.exists()) {
            log("root not exists: " + root_path);
            root.mkdirs();
        }
        File data_file  = new File(root_path + inst_id + "_data");
        File index_file = new File(root_path + inst_id + "_index");
        File temp_file  = new File(root_path + inst_id + "_temp");
        this.data_channel = open(data_file, FC_OPS);
        this.index_channel = open(index_file, FC_OPS);
        this.temp_channel = open(temp_file, FC_OPS);
        this.data_reader = new DioReader(data_file, "r");
        this.data_writer = new DioReader(data_file, "rw");
        FileChannel index_channel = this.index_channel;
        FileChannel data_channel  = this.data_channel;
        log("d_size: " + data_file.length() + ", i_size: " + index_file.length() + ", t_size: " + temp_file.length());
        try {
            long file_size = index_channel.size();
            if (file_size == 0) {
                log("open partition: " + inst_id + ",path: " + data_file.getAbsolutePath());
                int              size      = DATA_COUNT * KEY_LEN;
                MappedByteBuffer index_buf = map(index_channel, 0, size);
                MappedByteBuffer temp_buf  = map(temp_channel, 0, TEMP_FILE_LEN * VALUE_LEN);
                index_buf.position(KEY_LEN);
                this.index_buf = index_buf;
                this.temp_buf = temp_buf;
                this.temp_buf_addr = Unsafe.address(temp_buf);
            } else {
                log("load partition: " + inst_id + ",path: " + data_file.getAbsolutePath());
                MappedByteBuffer index_buf = map(index_channel, 0, file_size);
                MappedByteBuffer temp_buf  = map(temp_channel, 0, TEMP_FILE_LEN * VALUE_LEN);
                long             address   = Unsafe.address(index_buf);
                long             end       = file_size + address;
                long             index_pos = Unsafe.getLong(address);
                log("index pos: " + index_pos);
                if (index_pos > 0) {
                    long index_size = (index_pos - 8) >>> 3;
                    log("index size: " + index_size);
                    index_buf.position((int) (index_pos));
                    long expect_pos  = 1L * index_size * VALUE_LEN;
                    long recover_len = expect_pos - data_channel.size();
                    if (recover_len > 0) {
                        ByteBuffer temp = temp_buf.duplicate();
                        temp.limit((int) recover_len);
                        data_channel.write(temp);
                    } else {
                        data_channel.position(expect_pos);
                    }
                    this.data_writer.set_write_pos(expect_pos);
                } else {
                    index_buf.position(KEY_LEN);
                }
                this.temp_buf = temp_buf;
                this.temp_buf_addr = Unsafe.address(temp_buf);
                this.index_buf = index_buf;
            }
        } catch (IOException e) {
            printException(e);
            throw new RuntimeException(e);
        }
        return true;
    }

    public void set(final long key, final ByteBuf value) throws KVSException {
        MappedByteBuffer index_buf    = this.index_buf;
        MappedByteBuffer temp_buf     = this.temp_buf;
        int              temp_buf_pos = temp_buf.position();
        long             src_addr     = value.address() + value.absReadIndex();
        long             dst_addr     = temp_buf_addr + temp_buf_pos;
        Unsafe.copyMemory(src_addr, dst_addr, VALUE_LEN);
        value.skipRead(VALUE_LEN);
        temp_buf.position(temp_buf_pos + VALUE_LEN);
        if (!temp_buf.hasRemaining()) {
            try {
                ByteBuffer temp = temp_buf.duplicate();
                temp.flip();
                long st = System.nanoTime();
                data_writer.write(temp);
                long past = System.nanoTime() - st;
                data_write_cost += past;
            } catch (IOException e) {
                printException(e);
                throw new RuntimeException(e);
            }
            temp_buf.position(0);
        }
        index_buf.putLong(key);
        index_buf.putLong(0, index_buf.position());
    }

    public void read(Channel ch, long pos, ByteBuf val) {
        flush();
        try {
            data_reader.native_read(ch, val, pos, READ_BLOCK_SIZE);
        } catch (IOException e) {
            printException(e);
        }
        if (DEBUG && inst_id == 0) {
            log("read data: " + pos);
        }
    }

    public void close() {
        if (!p_init) {
            log("not init, ignore close");
            return;
        }
        p_init = false;
        log("close partition: " + inst_id + ", write cost: " + (data_write_cost / 1000_000));
        release_mapped_buf(this.index_buf);
        release_mapped_buf(this.temp_buf);
        Util.close(data_channel);
        Util.close(index_channel);
        data_write_cost = 0;
        temp_buf = null;
        index_buf = null;
    }

    public void flush() {
        ByteBuffer temp_buf = this.temp_buf;
        if (temp_buf.position() > 0) {
            log("flush partition: " + inst_id);
            ByteBuffer temp = temp_buf.duplicate();
            temp.flip();
            try {
                this.data_writer.write(temp);
            } catch (IOException e) {
                printException(e);
            }
            temp_buf.position(0);
        }
    }

    static final class DioReader implements Closeable {

        static final DirectIOLib DIRECT_IO_LIB = DirectIOLib.getLibForPath("./temp/test_direct");

        final ByteBuffer             read_buf      = DirectIOUtils.allocateForDirectIO(DIRECT_IO_LIB, READ_BLOCK_SIZE);
        final long                   read_buf_addr = Unsafe.address(read_buf);
        final DirectRandomAccessFile dio;

        long write_pos;
        long start_pos = -1;
        long end_pos   = -1;

        DioReader(File file, String mode) {
            this.dio = open_dio(file, mode);
        }

        void native_read(Channel ch, ByteBuf dst, long pos, int len) throws IOException {
            ByteBuffer read_buf = this.read_buf;
            int        read     = dio.read(read_buf, pos);
            read_buf.flip();
            ByteBuf data = ByteBuf.wrap(read_buf);
            data.readIndex(0);
            data.writeIndex(read);
            data.retain();
            dst.skipWrite(3);
            dst.writeIntLE(read);
            dst.retain();
            ch.write(dst);
            ch.writeAndFlush(data);
        }

        void full_read(DirectRandomAccessFile dio, ByteBuffer read_buf, long read_pos) throws IOException {
            for (; read_buf.hasRemaining(); ) {
                int len = dio.read(read_buf, read_pos);
                if (len == -1) {
                    break;
                }
                read_pos += len;
            }
        }

        void write(byte[] data, long pos) throws IOException {
            read_buf.clear();
            read_buf.put(data);
            read_buf.flip();
            dio.write(read_buf, pos);
        }

        void write(ByteBuffer buf) throws IOException {
            int len = dio.write(buf, write_pos);
            write_pos += len;
        }

        void set_write_pos(long pos) {
            this.write_pos = pos;
        }

        DirectRandomAccessFile open_dio(File file, String mode) {
            try {
                return new DirectRandomAccessFile(file, mode);
            } catch (IOException e) {
                printException(e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            Unsafe.free(read_buf_addr);
        }
    }

}
