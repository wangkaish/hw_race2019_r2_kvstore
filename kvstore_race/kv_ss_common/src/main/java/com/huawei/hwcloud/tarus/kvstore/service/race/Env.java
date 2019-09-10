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

/**
 * @author: wangkai
 **/
public class Env {

    public static final boolean ONLINE               = false;
    public static final boolean DEBUG                = false;
    public static final int     PORT                 = 9571;
    public static final int     LRU_SIZE             = 14;
    public static final int     KV_INSTANCE          = 16;
    public static final int     VALUE_LEN            = 1024 * 4;
    public static final int     DATA_COUNT           = ONLINE ? 1024 * 4400 : 1024 * 66;
    public static final int     OFFLINE_DATA_COUNT   = 1024 * 64;
    public static final int     KEY_LEN              = 8;
    public static final int     DATA_1M              = 1024 * 1024;
    public static final int     DATA_256K            = 1024 * 256;
    public static final int     DATA_4K              = 1024 * 4;
    public static final int     READ_BLOCK_SIZE      = ONLINE ? 1024 * 1024 * 4 : 1024 * 1024;
    public static final int     DATA_COUNT_PER_BLOCK = READ_BLOCK_SIZE / VALUE_LEN;
    public static final long    TIMEOUT              = ONLINE ? 9000 : 99999999;
    public static final String  PATH;


    static {
        if (ONLINE) {
            PATH = "/data/kvstore_admin/kv_store_admin/target/kv_store_admin/data/kk_db/";
        } else {
            PATH = "/home/test/temp/huawei_race/kk_db/";
        }
    }

}
