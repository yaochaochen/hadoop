package com.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

public class HDFSApp {

    public static final String HDFS_PATH = "hdfs://192.168.100.177:8020";

    FileSystem fileSystem = null;
    Configuration configuration = null;
    @Before
    public void setUp() throws Exception {
        configuration = new Configuration();
        configuration.set("dfs.replication", "1");

        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration, "hadoop");


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

    /**
     * 重新命名文件
     * @throws Exception
     */
     @Test
      public void rename() throws Exception {
          Path path = new Path("/hdfsapi/test/a.txt");
          Path newPath = new Path("/hdfsapi/test/b.txt");
          boolean res = fileSystem.rename(path, newPath);
         System.out.print(res);

     }

    /**
     * 从本地拷贝到HDFS上
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile() throws Exception {
         Path local = new Path("/project/hadoop-train-v2/hadoop-train-v2.iml");
         Path copy = new Path("/hdfsapi/test/");
         fileSystem.copyFromLocalFile(local, copy);

     }

     @Test
    public void copyBigFromLocalFile() throws Exception {
         InputStream in = new BufferedInputStream(new FileInputStream(new File("/Users/yaochaochen/Downloads/Motivation-0.2.1.zip")));
         FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/jdk.tgz"), new Progressable() {
             @Override
             public void progress() {
                 System.out.println("/");
             }
         });
         IOUtils.copyBytes(in, out, 4096);
     }

    /**
     * 拷贝HDFS文件到本地：下载
     */
    @Test
    public void copyToLocalFile() throws Exception {
        Path src = new Path("/hdfsapi/test/hello.txt");
        Path dst = new Path("/Users/yaochaochen/Downloads");
        fileSystem.copyToLocalFile(src, dst);
    }
    /**
     * 查看目标文件夹下的所有文件
     */
    @Test
    public void listFiles() throws Exception {
        FileStatus[] statuses = fileSystem.listStatus(new Path("/hdfsapi/test"));

        for(FileStatus file : statuses) {
            String isDir = file.isDirectory() ? "文件夹" : "文件";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long length = file.getLen();
            String path = file.getPath().toString();


            System.out.println(isDir + "\t" + permission
                    + "\t" + replication + "\t" + length
                    + "\t" + path
            );
        }

    }
    /**
     * 递归查看目标文件夹下的所有文件
     */
    @Test
    public void listFilesRecursive() throws Exception {

        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/hdfsapi/test"), true);

        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            String isDir = file.isDirectory() ? "文件夹" : "文件";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long length = file.getLen();
            String path = file.getPath().toString();


            System.out.println(isDir + "\t" + permission
                    + "\t" + replication + "\t" + length
                    + "\t" + path
            );
        }
    }
    /**
     * 查看文件块信息
     */
    @Test
    public void getFileBlockLocations() throws Exception {

        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/hdfsapi/test/jdk.tgz"));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus,0,fileStatus.getLen());

        for(BlockLocation block : blocks) {

            for(String name: block.getNames()) {
                System.out.println(name +" : " + block.getOffset() + " : " + block.getLength() + " : " + block.getHosts());
            }
        }
    }
    /**
     * 删除文件
     */
    @Test
    public void delete() throws Exception {
        boolean result = fileSystem.delete(new Path("/hdfsapi/test/jdk.tgz"), true);
        System.out.println(result);
    }
}
