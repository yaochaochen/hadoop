package com.bigdata.hdfs.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * Map 的输出到reduce端 是按照相同的key分发到一个reduce上
     * reduce1 ===> (hello,1) (hello,1) (hello,1) --> (hello,<1,1,1>)
     * reduce2 ===> (word 1) (word 1) ==> (word,<1,1>)
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            IntWritable intWritable = iterator.next();
            count += intWritable.get();
        }
        context.write(key, new IntWritable(count));
    }

}
