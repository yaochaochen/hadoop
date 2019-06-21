package com.bigdata.hdfs.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;


public class AccessApp {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.100.177:8020");

        Job job = Job.getInstance(configuration);
        job.setJarByClass(AccessApp.class);

        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReduce.class);

       // job.setNumReduceTasks(3);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.100.177:8020"),configuration,"hadoop");
        Path path = new Path("/access/output");
        //HDFS上存在该目录执行删除
        if (fileSystem.exists(path)) {
            fileSystem.delete(path,true);
        }
        FileInputFormat.setInputPaths(job, new Path("/access/input"));
        FileOutputFormat.setOutputPath(job, path);
        job.waitForCompletion(true);
    }
}
