package com.bigdata.hdfs.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN: Map任务读取数据的key类型，offset，是每行数据的偏移量，long
 * VALUEIN： Map任务读取数据的value类型，其实就是一行行的字符串，String
 * KEYOUT： map方法自定义实现输出的key的类型，String
 * VALUEOUT：map方法自定义实现输出的value的类型，Integer
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       //按照指定分隔符切分
        String[] words = value.toString().split("\t");
        for (String word : words) {
            //统计词频（hello，1）
            context.write(new Text(word.toLowerCase()), new IntWritable(1));
        }
    }
}
