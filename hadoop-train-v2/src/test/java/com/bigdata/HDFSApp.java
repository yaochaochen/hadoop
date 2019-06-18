package com.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import sun.nio.ch.IOUtil;

import java.net.URI;

public class HDFSApp {

    public static final String HDFS_PATH = "hdfs://123.56.22.81:8020";

    FileSystem fileSystem = null;
    Configuration configuration = null;
    @Before
    public void setUp() throws Exception {
        configuration = new Configuration();
        configuration.set("dfs.replication", "1");

        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration, "root");


    }

    /*** 创建HDFS文件夹
     */
    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /**
     * 创建文件
     */
    @Test
    public void create() throws Exception {
        FSDataOutputStream outputStream = fileSystem.create(new Path("/test/a.txt"));
        outputStream.writeUTF("hello Hadoop");
        outputStream.flush();
        outputStream.close();
    }
    /**
     * 查看HDFS内容
     * @throws Exception
     */
    @Test
    public void  text() throws Exception {
        FSDataInputStream inputStream = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(inputStream, System.out,1024);

    }

}
