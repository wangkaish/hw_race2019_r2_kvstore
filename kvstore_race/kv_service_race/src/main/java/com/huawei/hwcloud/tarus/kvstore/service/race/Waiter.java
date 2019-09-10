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

import static com.huawei.hwcloud.tarus.kvstore.service.race.Env.TIMEOUT;
import static com.huawei.hwcloud.tarus.kvstore.service.race.Env.VALUE_LEN;

/**
 * @program: kvstore_java_se
 * @description:
 * @author: wangkai
 * @create: 2019-08-16 15:09
 **/
public class Waiter {

    private       boolean isDone;
    private       boolean success;
    private       int     len;
    private final Object  lock;
    private final byte[]  response = new byte[VALUE_LEN];

    public Waiter() {
        this(null);
    }

    public Waiter(Object lock) {
        if (lock == null) {
            lock = this;
        }
        this.lock = lock;
    }

    public void reset() {
        this.isDone = false;
        this.success = false;
    }

    public void get() {
        get(TIMEOUT);
    }

    public void get(long timeout) {
        synchronized (lock) {
            if (isDone) {
                return;
            }
            com.firenio.common.Util.wait(lock, timeout);
        }
        if (!isDone) {
            Util.log("timeout......");
            throw new RuntimeException("timeout");
        }
    }

    public void call() {
        call(true);
    }

    public void call(boolean success) {
        synchronized (lock) {
            if (isDone) {
                return;
            }
            this.isDone = true;
            this.success = success;
            lock.notify();
        }
    }

    public byte[] getResponse() {
        return response;
    }

    public boolean isSuccess() {
        return success;
    }
}
