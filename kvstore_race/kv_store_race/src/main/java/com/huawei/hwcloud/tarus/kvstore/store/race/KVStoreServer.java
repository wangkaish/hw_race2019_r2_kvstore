package com.huawei.hwcloud.tarus.kvstore.store.race;

import java.io.File;

import com.firenio.common.FileUtil;
import com.firenio.common.Util;
import com.firenio.component.Channel;
import com.firenio.component.ChannelAcceptor;
import com.firenio.component.ChannelEventListenerAdapter;
import com.firenio.component.NioEventLoopGroup;
import com.firenio.component.SocketOptions;
import com.firenio.log.LoggerFactory;
import com.huawei.hwcloud.tarus.kvstore.common.KVStoreCheck;
import com.huawei.hwcloud.tarus.kvstore.store.race.ServerCodec.ChannelCache;

import static com.huawei.hwcloud.tarus.kvstore.service.race.Env.*;
import static com.huawei.hwcloud.tarus.kvstore.service.race.Util.log;
import static com.huawei.hwcloud.tarus.kvstore.service.race.Util.printException;

public class KVStoreServer implements KVStoreCheck {

    static {
        LoggerFactory.setEnableSLF4JLogger(false);
    }

    static final EngineKVStoreRace[] INSTANCES = new EngineKVStoreRace[KV_INSTANCE];

    static {
        for (int i = 0; i < KV_INSTANCE; i++) {
            INSTANCES[i] = new EngineKVStoreRace();
        }
    }

    ChannelAcceptor context;

    @Override
    public double execute() {
        String v = Util.getStringProperty("kvs.clear.data");
        if ("0".equals(v)) {
            log("clean data.....");
            File   file = new File(PATH);
            File[] fs   = file.listFiles();
            if (fs != null) {
                for (File f : fs) {
                    log("clean data: " + f.getAbsolutePath());
                }
                FileUtil.cleanDirectory(file);
            }
            log("clean data finish....");
        }
        NioEventLoopGroup group = new NioEventLoopGroup();
        group.setEnableMemoryPool(false);
        group.setIdleTime(Integer.MAX_VALUE);
        group.setChannelReadBuffer(DATA_1M);
        group.setEventLoopSize(KV_INSTANCE);
        group.setWriteBuffers(1);
        ChannelAcceptor context = new ChannelAcceptor(group, PORT);
        context.addProtocolCodec(new ServerCodec());
        context.addChannelEventListener(new ChannelEventListenerAdapter() {

            @Override
            public void channelOpened(Channel ch) throws Exception {
                ch.setOption(SocketOptions.TCP_NODELAY, 1);
                ch.setOption(SocketOptions.SO_RCVBUF, DATA_1M);
                ch.setOption(SocketOptions.SO_SNDBUF, DATA_1M);
                int read_buf = ch.getOption(SocketOptions.SO_RCVBUF);
                int send_buf = ch.getOption(SocketOptions.SO_SNDBUF);
                log("ch open: " + ch.toString() + ", r_buf: " + read_buf + ", s_buf: " + send_buf);
                ch.setAttachment(new ChannelCache());
            }

            @Override
            public void channelClosed(Channel ch) {
                ChannelCache cache = (ChannelCache) ch.getAttachment();
                Util.release(cache.send_buf);
                Util.release(cache.send_buf);
                log("ch close: " + ch.toString());
            }
        });
        log("ready to bind...");
        try {
            context.bind();
            log("bind success");
        } catch (Exception e) {
            printException(e);
        }
        this.context = context;
        return 0.1;
    }

}
