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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.huawei.hwcloud.tarus.kvstore.service.race.Util.printException;

/**
 * @author: wangkai
 **/
public final class AtomicRacer {

    static final AtomicIntegerFieldUpdater<AtomicRacer> SEQ_UPDATER;
    final        int                                    sleep;
    final        int                                    max;
    volatile     int                                    seq;

    static {
        SEQ_UPDATER = AtomicIntegerFieldUpdater.newUpdater(AtomicRacer.class, "seq");
    }

    public AtomicRacer(int sleep, int max) {
        this.sleep = sleep;
        this.max = max;
    }

    void acquire() {
        for (; ; ) {
            int seq = this.seq;
            if (seq < max) {
                int next = seq + 1;
                if (SEQ_UPDATER.compareAndSet(this, seq, next)) {
                    break;
                }
            } else {
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException e) {
                    printException(e);
                }
            }
        }
    }

    void release() {
        for (; ; ) {
            int seq  = this.seq;
            int next = seq - 1;
            if (SEQ_UPDATER.compareAndSet(this, seq, next)) {
                break;
            }
        }
    }

}
