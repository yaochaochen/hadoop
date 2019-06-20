package com.bigdata.hdfs.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * 执行到HDFS上执行 将输出存入HDFS
 */
public class WordCountApp {


    public static void main(String[] args) throws Exception{
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.100.177:8020");

        Job job = Job.getInstance(configuration);
        job.setJarByClass(WordCountApp.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.100.177:8020"),configuration,"hadoop");
        Path path = new Path("/wordcount/output");
         //HDFS上存在该目录执行删除
        if (fileSystem.exists(path)) {
            fileSystem.delete(path,true);
        }
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job,path);
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : -1);


    }


}
