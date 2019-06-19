package com.bigdata.hdfs.wc;

import java.util.HashMap;
import java.util.Map;

public class WcContext {

    private Map<Object, Object> cacheMap = new HashMap<>();

    public Map<Object, Object> getCacheMap() {
        return cacheMap;
    }

    /**
     * 写入
     * @param key
     * @param value
     */
    public void write(Object key, Object value) {
        cacheMap.put(key, value);
    }

    /***
     * 读取
     * @param key
     * @return
     */
    public Object get(Object key) {
        return cacheMap.get(key);
    }
}
