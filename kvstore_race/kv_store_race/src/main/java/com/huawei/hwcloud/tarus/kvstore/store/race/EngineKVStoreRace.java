package com.huawei.hwcloud.tarus.kvstore.store.race;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;

import com.firenio.buffer.ByteBuf;
import com.firenio.common.Unsafe;
import com.firenio.common.Util;
import com.firenio.component.Channel;
import com.firenio.component.Native;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;
import com.huawei.hwcloud.tarus.kvstore.service.race.LongIntMap;

import static com.huawei.hwcloud.tarus.kvstore.service.race.Env.*;
import static com.huawei.hwcloud.tarus.kvstore.service.race.Util.*;

public class EngineKVStoreRace {

    static final int          TEMP_FILE_LEN = ONLINE ? 256 : 3;
    static final OpenOption[] FC_OPS        = new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE};

    FileChannel      index_channel;
    FileChannel      temp_channel;
    DFileChannel     data_channel;
    MappedByteBuffer index_buf;
    MappedByteBuffer temp_buf;
    long             temp_buf_addr;
    long             data_write_cost = 0;
    boolean          p_init          = false;
    int              inst_id;

    public boolean init(final int inst_id) {
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
        this.index_channel = open(index_file, FC_OPS);
        this.temp_channel = open(temp_file, FC_OPS);
        this.data_channel = new DFileChannel(data_file);
        FileChannel index_channel = this.index_channel;
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
                    int  recover_len = (int) (expect_pos - data_channel.file_size());
                    if (recover_len > 0) {
                        data_channel.write(temp_buf_addr, recover_len);
                    }
                    data_channel.set_write_pos(expect_pos);
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

    public void write(final long key, final ByteBuf value) {
        ByteBuffer index_buf    = this.index_buf;
        ByteBuffer temp_buf     = this.temp_buf;
        int        temp_buf_pos = temp_buf.position();
        long       src_addr     = value.address() + value.absReadIndex();
        long       dst_addr     = temp_buf_addr + temp_buf_pos;
        Unsafe.copyMemory(src_addr, dst_addr, VALUE_LEN);
        value.skipRead(VALUE_LEN);
        temp_buf.position(temp_buf_pos + VALUE_LEN);
        if (!temp_buf.hasRemaining()) {
            long st = System.nanoTime();
            data_channel.write(temp_buf_addr, TEMP_FILE_LEN * VALUE_LEN);
            data_write_cost += System.nanoTime() - st;
            temp_buf.position(0);
        }
        index_buf.putLong(key);
        index_buf.putLong(0, index_buf.position());
    }

    public void read(Channel ch, long pos, ByteBuf val) {
        flush();
        data_channel.read(ch, val, pos, READ_BLOCK_SIZE);
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
        release_mapped_buf(index_buf);
        release_mapped_buf(temp_buf);
        Util.close(index_channel);
        Util.close(data_channel);
    }

    public void flush() {
        ByteBuffer temp_buf = this.temp_buf;
        if (temp_buf.position() > 0) {
            log("flush partition: " + inst_id);
            this.data_channel.write(temp_buf_addr, temp_buf.position());
            temp_buf.position(0);
        }
    }

    static final class DFileChannel implements Closeable {

        final long    read_buf_addr = Native.posix_memalign_allocate(READ_BLOCK_SIZE, 1024 * 4);
        final ByteBuf read_buf      = ByteBuf.wrap(read_buf_addr, READ_BLOCK_SIZE);
        final int     fd;

        long write_pos;

        DFileChannel(File file) {
            this.fd = Native.open(file.getAbsolutePath(), Native.O_RDWR | Native.O_CREAT | Native.O_DIRECT, 0755);
            this.write_pos = Native.file_length(fd);
        }

        void read(Channel ch, ByteBuf dst, long pos, int len) {
            Native.lseek(fd, pos, Native.SEEK_SET);
            ByteBuf data = this.read_buf;
            int     read = Native.read(fd, read_buf_addr, READ_BLOCK_SIZE);
            data.readIndex(0);
            data.writeIndex(read);
            data.retain();
            dst.skipWrite(3);
            dst.writeIntLE(read);
            dst.retain();
            ch.write(dst);
            ch.writeAndFlush(data);
        }

        void write(long address, int len) {
            int write_len = Native.write(fd, address, len);
            write_pos += write_len;
        }

        void set_write_pos(long pos) {
            this.write_pos = pos;
            Native.lseek(fd, pos, Native.SEEK_SET);
        }

        long file_size() {
            return write_pos;
        }

        @Override
        public void close() {
            Unsafe.free(read_buf_addr);
            Native.close(fd);
        }

    }

}
