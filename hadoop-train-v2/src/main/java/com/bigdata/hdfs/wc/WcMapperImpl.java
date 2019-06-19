package com.bigdata.hdfs.wc;

public class WcMapperImpl implements  WcMapper {
    @Override
    public void map(String line, WcContext wcContext) {
        String[] words = line.split("\t");
        for(String word : words) {
            Object val = wcContext.get(word);
            if(val == null) {
                wcContext.write(word, 1);
            }else {
                int va = Integer.parseInt(val.toString());
                wcContext.write(word, va+1);
            }
        }
    }
}
