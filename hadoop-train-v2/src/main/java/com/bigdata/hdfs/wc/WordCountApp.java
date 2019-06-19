package com.bigdata.hdfs.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Set;

/**
 * 基于HDFS的API完成词频统计
 * 1 读取HDFS上的文件
 * 2 业务处理 按照分隔符 ==>Mapper
 * 3输出
 */
public class WordCountApp {

    public static void main(String[] args) throws Exception{

        //读取文件
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.100.177:8020"), new Configuration(),"hadoop");

        Path inpath = new Path("/hdfsapi/test/hello.txt");
        RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listFiles(inpath,false);
        WcMapper mapper = new WcMapperImpl();
        WcContext context = new WcContext();
        while (remoteIterator.hasNext()) {
            LocatedFileStatus fileStatus = remoteIterator.next();
            FSDataInputStream in  = fileSystem.open(fileStatus.getPath());
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));
            String line = "";
            while ((line = bufferedReader.readLine()) != null) {
                //业务处理
                mapper.map(line, context);
            }
            bufferedReader.close();
            in.close();
        }
        //将结果存入Map
        Map<Object,Object> contextMap = context.getCacheMap();
        Path outpath = new Path("/wc/out/");
         FSDataOutputStream outputStream = fileSystem.create(new Path(outpath,"wc.out"));

        Set<Map.Entry<Object, Object>> entries = contextMap.entrySet();
        for (Map.Entry<Object, Object> entry : entries) {
            outputStream.write((entry.getKey().toString() + "\t" + entry.getValue() +"\n").getBytes());
          //  System.out.println( entry.getKey().toString() + "\t" + entry.getValue() +"\n");
        }
        outputStream.close();
        fileSystem.close();
        System.out.println("API统计词频成功");

    }


}
