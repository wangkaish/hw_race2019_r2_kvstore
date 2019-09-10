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

import java.util.Arrays;

import static com.huawei.hwcloud.tarus.kvstore.service.race.Util.clothCover;

/**
 * @author: wangkai
 **/
public final class LongIntMap {

    private final float  loadFactor;
    private       int    cap;
    private       long[] keys;
    private       int[]  values;
    private       int    scanSize;
    private       int    size;
    private       int    mask;
    private       int    scanIndex;
    private       int    limit;

    public LongIntMap() {
        this(16);
    }

    public LongIntMap(int cap) {
        this(cap, 0.75f);
    }

    public LongIntMap(int cap, float loadFactor) {
        cap = Math.max(16, cap);
        int c = clothCover(cap);
        this.cap = c;
        this.mask = c - 1;
        this.loadFactor = Math.min(loadFactor, 0.75f);
        this.keys = new long[c];
        this.values = new int[c];
        this.limit = (int) (c * loadFactor);
        Arrays.fill(keys, -1);
    }

    private static int indexOfKey(long[] keys, long key, int mask) {
        int index = (int) (key & mask);
        if (keys[index] == key) {
            return index;
        }
        if (keys[index] == -1) {
            return -1;
        }
        for (int i = index, cnt = keys.length; i < cnt; i++) {
            if (keys[i] == key) {
                return i;
            }
        }
        for (int i = 1; i < index; i++) {
            if (keys[i] == key) {
                return i;
            }
        }
        //will not happen
        return -1;

    }

    private static int indexOfFreeKey(long[] keys, long key, int mask) {
        int  index = (int) (key & mask);
        long _key  = keys[index];
        if (_key == -1 || _key == key) {
            return index;
        }
        for (int i = index, cnt = keys.length; i < cnt; i++) {
            _key = keys[i];
            if (_key == -1 || _key == key) {
                return i;
            }
        }
        for (int i = 1; i < index; i++) {
            _key = keys[i];
            if (_key == -1 || _key == key) {
                return i;
            }
        }
        //will not happen
        return -1;

    }

    public void scan() {
        this.scanSize = 0;
        this.scanIndex = -1;
    }

    public int putIfAbsent(long key, int value) {
        return putVal(key, value, true);
    }

    private int putVal(long key, int value, boolean absent) {
        int res = put0(key, value, mask, keys, values, absent);
        if (res == -1) {
            grow();
            return -1;
        }
        return res;
    }

    public int put(long key, int value) {
        return putVal(key, value, false);
    }

    private int scan(long[] keys, int index) {
        for (int i = index + 1, cnt = keys.length; i < cnt; i++) {
            if (keys[i] != -1) {
                return i;
            }
        }
        return keys.length;
    }

    public int scanIndex() {
        return scanIndex;
    }

    public boolean hasNext() {
        return next() != -1;
    }

    private int next() {
        if (scanSize < size) {
            long[] keys  = this.keys;
            int    index = this.scanIndex + 1;
            int    cap   = this.cap;
            for (; index < cap; index++) {
                if (keys[index] != -1) {
                    break;
                }
            }
            this.scanSize++;
            this.scanIndex = index;
            return index;
        }
        return -1;
    }

    public long key() {
        return keys[scanIndex];
    }

    public int value() {
        return values[scanIndex];
    }

    public long indexKey(int index) {
        return keys[index];
    }

    public int indexValue(int index) {
        return values[index];
    }

    private int put0(long key, int value, int mask, long[] keys, int[] values, boolean absent) {
        int index = indexOfFreeKey(keys, key, mask);
        if (keys[index] == key) {
            if (absent) {
                return values[index];
            } else {
                int old = values[index];
                values[index] = value;
                return old;
            }
        } else {
            keys[index] = key;
            values[index] = value;
            return -1;
        }
    }

    private void grow() {
        size++;
        if (size > limit) {
            int    cap    = clothCover(this.cap + 1);
            int    mask   = cap - 1;
            long[] keys   = new long[cap];
            int[]  values = new int[cap];
            int    limit  = (int) (cap * loadFactor);
            Arrays.fill(keys, -1);
            scan();
            for (; ; ) {
                int index = next();
                if (index == -1) {
                    break;
                }
                put0(indexKey(index), indexValue(index), mask, keys, values, false);
            }
            this.cap = cap;
            this.mask = mask;
            this.keys = keys;
            this.values = values;
            this.limit = limit;
        }
    }

    public int get(long key) {
        int index = indexOfKey(keys, key, mask);
        if (index == -1) {
            return -1;
        }
        return values[index];
    }

    private void remove_at(int index) {
        long[] keys        = this.keys;
        int[]  values      = this.values;
        int    cap         = this.cap;
        int    mask        = this.mask;
        int    write_index = index;
        int    read_index  = index + 1;
        for (; read_index < cap; read_index++) {
            long key = keys[read_index];
            if (key == -1) {
                break;
            }
            if ((key & mask) == read_index) {
                continue;
            }
            keys[write_index] = key;
            values[write_index] = values[read_index];
            write_index = read_index;
        }
        keys[write_index] = -1;
        values[write_index] = -1;
    }

    public void finishScan() {
        scanIndex = -1;
    }

    public int remove(long key) {
        int index = indexOfKey(keys, key, mask);
        if (index == -1) {
            return -1;
        }
        int v = values[index];
        remove_at(index);
        size--;
        if (index <= scanIndex) {
            scanSize--;
            scanIndex--;
        }
        return v;
    }

    public String toString() {
        if (size == 0) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder(4 * size);
        for (int i = 0; i < values.length; ++i) {
            int value = values[i];
            if (value != -1) {
                sb.append(sb.length() == 0 ? "{" : ", ");
                sb.append(keys[i]).append('=').append(value);
            }
        }
        return sb.append('}').toString();
    }

    public int conflict() {
        int s = 0;
        scan();
        for (; ; ) {
            int index = next();
            if (index == -1) {
                break;
            }
            if ((key() & mask) != index) {
                s++;
            }
        }
        return s;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public int size() {
        return size;
    }

    public void clear() {
        Arrays.fill(keys, -1);
        size = 0;
    }

}
