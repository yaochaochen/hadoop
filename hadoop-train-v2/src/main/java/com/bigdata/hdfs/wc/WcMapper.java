package com.bigdata.hdfs.wc;

/**
 * Mapeer
 */
public interface WcMapper {

    public void map(String line, WcContext wcContext);
}
