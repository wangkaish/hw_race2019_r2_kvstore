/*
 * Copyright 2015 The FireNio Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.huawei.hwcloud.tarus.kvstore.service.race;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.OpenOption;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import moe.cnkirito.kdio.DirectRandomAccessFile;
import sun.nio.ch.DirectBuffer;

import com.firenio.buffer.ByteBuf;
import com.firenio.common.Unsafe;
import com.firenio.component.Channel;

/**
 * @author: wangkai
 **/
public class Util {

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    private static ThreadLocal threadLocal = ThreadLocal.withInitial(() -> new SimpleDateFormat(DATE_FORMAT));

    public static DateFormat getDateFormat() {
        return (DateFormat) threadLocal.get();
    }

    public static String date() {
        return getDateFormat().format(new Date());
    }

    public static void log(String msg) {
        System.out.println(date() + ":::: " + msg);
    }

    public static long now() {
        return System.currentTimeMillis();
    }

    public static long past(long start) {
        return now() - start;
    }

    public static void printException(Throwable e) {
        e.printStackTrace(System.out);
    }

    public static void release_mapped_buf(MappedByteBuffer buf) {
        if (buf != null && buf.position() > 0) {
            buf.force();
            ((DirectBuffer) buf).cleaner().clean();
        }
    }

    public static void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                printException(e);
            }
        }
    }

    public static void collation_buf(ByteBuf buf) {
        if (buf.hasReadableBytes() && buf.readIndex() > 0) {
            long address         = buf.address();
            int  remain          = buf.readableBytes();
            int  abs_read_index  = buf.absReadIndex();
            int  abs_write_index = buf.absWriteIndex();
            long src_addr        = address + abs_read_index;
            Unsafe.copyMemory(src_addr, address, remain);
            buf.readIndex(0);
            buf.writeIndex(remain);
        }
    }

    public static long string2long(String v) {
        long sum = 0;
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            sum = sum * 10 + (c - '0');
        }
        return sum;
    }

    public static MappedByteBuffer map(FileChannel ch, long pos, long size) {
        try {
            MappedByteBuffer buf = ch.map(MapMode.READ_WRITE, pos, size);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            return buf;
        } catch (IOException e) {
            printException(e);
            throw new RuntimeException(e);
        }
    }

    public static FileChannel open(File file, OpenOption... options) {
        try {
            FileChannel channel = FileChannel.open(file.toPath(), options);
            channel.position(channel.size());
            return channel;
        } catch (IOException e) {
            printException(e);
            throw new RuntimeException(e);
        }
    }

    public static DirectRandomAccessFile openDioR(File file) {
        try {
            return new DirectRandomAccessFile(file, "r");
        } catch (IOException e) {
            printException(e);
            throw new RuntimeException(e);
        }
    }

    public static int clothCover(int v) {
        int n = 2;
        for (; n < v; )
            n <<= 1;
        return n;
    }

    public static byte get_header(int ins_id, int action) {
        return (byte) ((ins_id << 4) | action);
    }

    public static int get_ins_id(int header) {
        return (header >> 4);
    }

    public static int get_action(int header) {
        return header & 0xf;
    }

    public static ByteBuf getChannelByteBuf(Channel channel) {
        return ((ByteBuf) channel.getAttachment());
    }

    public static void setChannelByteBuf(Channel channel, ByteBuf buf) {
        channel.setAttachment(buf);
    }

    public static void cacheByteBuf(Channel channel, ByteBuf src, ByteBuf cache) {
        if (src != cache) {
            cache.writeBytes(src);
            channel.setAttachment(cache);
        }
    }

    public static void removeChannelByteBuf(Channel channel) {
        channel.setAttachment(null);
    }

    public static void obj_wait(Object o) {
        obj_wait(o, 6000);
    }

    public static void obj_wait(Object o, long timeout) {
        try {
            o.wait(timeout);
        } catch (InterruptedException e) {
            printException(e);
        }
    }

    public static long channel_size(FileChannel channel) {
        try {
            return channel.size();
        } catch (IOException e) {
            printException(e);
            return -1;
        }
    }

    public static void print_jvm_args() {
        MemoryMXBean memorymbean = ManagementFactory.getMemoryMXBean();
        log("堆内存信息: " + memorymbean.getHeapMemoryUsage());
        log("方法区内存信息: " + memorymbean.getNonHeapMemoryUsage());

        List<String> inputArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();
        log("\n#####################运行时设置的JVM参数#######################");
        log(String.valueOf(inputArgs));

        log("\n#####################运行时内存情况#######################");
        long totle = Runtime.getRuntime().totalMemory();
        log("总的内存量 [" + totle + "]");
        long free = Runtime.getRuntime().freeMemory();
        log("空闲的内存量 [" + free + "]");
        long max = Runtime.getRuntime().maxMemory();
        log("最大的内存量 [" + max + "]");
    }


}
