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
public final class ActionType {

    public static final int ACTION_INIT     = 1;
    public static final int ACTION_SET      = 2;
//    public static final int ACTION_GET      = 3;
    public static final int ACTION_FLUSH    = 4;
    public static final int ACTION_CLOSE    = 5;
    public static final int ACTION_READ     = 6;

}
