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

import java.nio.ByteBuffer;

import com.firenio.buffer.ByteBuf;
import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.util.BufferUtil;

import static com.huawei.hwcloud.tarus.kvstore.service.race.Util.*;

/**
 * @author: wangkai
 **/
public class TestService {


    public static void main(String[] args) throws Exception {
        String key      = "124554051584";
        long   long_key = string2long(key);
        int    p        = (int) (long_key >>> 32);
        int    pk       = (int) long_key & 0xffffffff;
        System.out.println(p);
        System.out.println(pk);

        test_read_write();

        //        URI uri = new URI("tcp://192.168.30.193");
        //        System.out.println(uri.getHost());


        System.out.println("complete");


    }

    static void test_collation_buf(){
        ByteBuf buf = ByteBuf.buffer(1024);
        buf.skipWrite(1);
        buf.writeBytes("abc123".getBytes());
        buf.skipRead(1);
        Util.collation_buf(buf);
        System.out.println(new String(buf.readBytes()));
    }


    static void test_read_write() {
        KVService r = new KVService();

        String dir = Env.PATH;

        int prefix = 0;

        int count = Env.OFFLINE_DATA_COUNT;

        r.init(dir, prefix);

//                write(r, prefix, count);

        read(r, prefix, count);

        read_pre(r, prefix, count);

        r.close();

        r.close();
    }

    private static void write(KVService r, int prefix, int times) {
        byte[]     buf        = new byte[4096];
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        long       begin      = System.currentTimeMillis();
        long       i;
        for (i = 0; i < (long) times; ++i) {
            long   key      = buildKey(prefix, i);
            String keyStr   = String.valueOf(key);
            byte[] keyBytes = BufferUtil.stringToBytes(keyStr);
            byteBuffer.clear();
            byteBuffer.putInt(prefix).putInt((int) i);
            byteBuffer.flip();
            byte[] check = byteBuffer.array();
            System.arraycopy(check, 0, buf, 0, check.length);
            try {
                r.set(keyStr, buf);
            } catch (Exception e) {
                printException(e);
            }
        }
    }

    private static final long buildKey(int prefix, long idx) {
        long key = (long) prefix;
        key <<= 32;
        return key + idx;
    }

    private static void read(KVService r, int prefix, int times) {
        int        error  = 0;
        long       begin  = System.currentTimeMillis();
        ByteBuffer buffer = ByteBuffer.allocate(4096);

        for (int i = 0; i < times; ++i) {
            long        key     = buildKey(prefix, (long) i);
            String      keyStr  = String.valueOf(key);
            Ref<byte[]> val_ref = Ref.of(byte[].class);
            r.get(keyStr, val_ref);
            byte[] val_buf = (byte[]) val_ref.getValue();
            if (val_buf == null) {
                log("read data, key=[{}], data is null: " + keyStr);
                ++error;
            } else {
                int val_size = val_buf.length;
                buffer.clear();
                buffer.put(val_buf);
                buffer.flip();
                if (prefix != buffer.getInt() || i != buffer.getInt()) {
                    log("read data, key=[{}], val error: " + keyStr);
                    ++error;
                }
            }
        }
    }

    private static void read_pre(KVService r, int prefix, int times) {
        int        error  = 0;
        long       begin  = System.currentTimeMillis();
        ByteBuffer buffer = ByteBuffer.allocate(4096);

        for (int i = times - 1; i >= 0; --i) {
            long        key     = buildKey(prefix, (long) i);
            String      keyStr  = String.valueOf(key);
            Ref<byte[]> val_ref = Ref.of(byte[].class);
            r.get(keyStr, val_ref);
            byte[] val_buf = (byte[]) val_ref.getValue();
            if (val_buf == null) {
                log("read data, key=[{}], data is null: " + keyStr);
                ++error;
            } else {
                int val_size = val_buf.length;
                buffer.clear();
                buffer.put(val_buf);
                buffer.flip();
                if (prefix != buffer.getInt() || i != buffer.getInt()) {
                    log("read data, key=[{}], val error: " + keyStr);
                    ++error;
                }
            }
        }
    }


}
