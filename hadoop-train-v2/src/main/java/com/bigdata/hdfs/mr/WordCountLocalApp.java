package com.bigdata.hdfs.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 提交本地执行 输出结果到本地
 */
public class WordCountLocalApp {

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //设置启动主类
        job.setJarByClass(WordCountLocalApp.class);
        //设置Mapper和Reduce的处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //设置输出的key和 value
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("input/wc.input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
       boolean result = job.waitForCompletion(true);
       System.exit(result ? 0 : -1);

    }

}
