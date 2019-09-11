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
package com.huawei.hwcloud.tarus.kvstore.store.race;

import java.nio.ByteBuffer;

import com.firenio.buffer.ByteBuf;
import com.firenio.component.Channel;
import com.firenio.component.Frame;
import com.firenio.component.NioEventLoop;
import com.firenio.component.ProtocolCodec;

import static com.huawei.hwcloud.tarus.kvstore.service.race.ActionType.*;
import static com.huawei.hwcloud.tarus.kvstore.service.race.Env.*;
import static com.huawei.hwcloud.tarus.kvstore.service.race.Util.*;

/**
 * @program: kvstore_java_se
 * @description:
 * @author: wangkai
 * @create: 2019-08-16 16:08
 **/
public class ServerCodec extends ProtocolCodec {

    @Override
    public Frame decode(Channel ch, ByteBuf src) {
        int               header   = src.readUnsignedByte();
        int               action   = get_action(header);
        int               ins_id   = get_ins_id(header);
        ChannelCache      cache    = (ChannelCache) ch.getAttachment();
        ByteBuf           send_buf = cache.send_buf.clear();
        EngineKVStoreRace store    = KVStoreServer.INSTANCES[ins_id];
        if (action == ACTION_READ) {
            if (src.readableBytes() < 11) {
                log("read data not complete, less than 8 byte: " + src.readableBytes());
                src.skipRead(-1);
                return null;
            }
            src.skipRead(3);
            long pos = src.readLongLE();
            send_buf.writeByte((byte) header);
            store.read(ch, pos, send_buf);
        } else if (action == ACTION_SET) {
            if (src.readableBytes() < VALUE_LEN + KEY_LEN + 3) {
                src.skipRead(-1);
                return null;
            }
            src.skipRead(3);
            long key = src.readLongLE();
            store.write(key, src);
            send_buf.writeByte((byte) header);
            send_buf.retain();
            ch.writeAndFlush(send_buf);
        } else if (action == ACTION_INIT) {
            log("received init: " + ins_id);
            store.init(ins_id);
            ByteBuffer index_buf     = store.index_buf;
            int        len           = index_buf.position();
            ByteBuf    index_buf_buf = ByteBuf.wrap(index_buf);
            send_buf.writeByte((byte) header);
            send_buf.skipWrite(3);
            send_buf.writeInt(len - KEY_LEN);
            index_buf_buf.readIndex(KEY_LEN);
            index_buf_buf.writeIndex(len);
            if (index_buf_buf.hasReadableBytes()) {
                index_buf_buf.retain();
                send_buf.retain();
                ch.write(send_buf);
                ch.writeAndFlush(index_buf_buf);
            } else {
                send_buf.retain();
                ch.writeAndFlush(send_buf);
            }
            log("send init len: " + len + ", inst_id: " + ins_id);
        } else if (action == ACTION_FLUSH) {
            log("received flush: " + ins_id);
            store.flush();
            send_buf.writeByte((byte) header);
            send_buf.retain();
            ch.writeAndFlush(send_buf);
            log("send flush finish: " + ins_id);
        } else if (action == ACTION_CLOSE) {
            log("received close: " + ins_id);
            store.close();
            send_buf.writeByte((byte) header);
            send_buf.retain();
            ch.writeAndFlush(send_buf);
            log("send close finish: " + ins_id);
        } else {
            log("unknown action: " + header);
        }
        return null;
    }

    @Override
    public String getProtocolId() {
        return "ServerCodec";
    }

    @Override
    public int getHeaderLength() {
        return 0;
    }

    @Override
    protected ByteBuf getPlainReadBuf(NioEventLoop el, Channel ch) {
        ChannelCache cache = (ChannelCache) ch.getAttachment();
        return cache.read_buf;
    }

    @Override
    protected void storePlainReadRemain(Channel ch, ByteBuf src) {
        ChannelCache cache    = (ChannelCache) ch.getAttachment();
        ByteBuf      read_buf = cache.read_buf;
        if (read_buf.readIndex() > 0) {
            read_buf.collation();
        }
    }

    @Override
    protected void readPlainRemain(Channel ch, ByteBuf dst) {

    }

    static class ChannelCache {
        final ByteBuf send_buf = ByteBuf.buffer(64);
        final ByteBuf read_buf = ByteBuf.buffer(1024 * 8);
    }

}
