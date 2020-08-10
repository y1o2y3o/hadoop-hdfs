package com.msb.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

public class TestHDFS {

    public Configuration conf = null;
    public FileSystem fs = null;

    @Before
    public void conn() throws IOException, InterruptedException {
        //System.setProperty("HADOOP_USER_NAME","god");
        conf = new Configuration(true);
        fs = FileSystem.get(URI.create("hdfs://mycluster/"),conf, "god");
    }

    @Test
    public void mkdir() throws IOException {
        Path dir = new Path("msb");
        if(fs.exists(dir)){
            fs.delete(dir, true);
        }
        fs.mkdirs(dir);
    }

    @Test
    public void upload() throws IOException {
        BufferedInputStream input = new BufferedInputStream(new FileInputStream(new File("./data/hello.txt")));
        Path outfile = new Path("/msb/out.txt");
        FSDataOutputStream output = fs.create(outfile);
        IOUtils.copyBytes(input, output, conf, true);
    }


    @Test
    public void blocks() throws IOException {
        Path file = new Path("/user/god/data.txt");
        FileStatus fss = fs.getFileStatus(file);
        BlockLocation[] blks = fs.getFileBlockLocations(fss, 0, fss.getLen());
        for(BlockLocation b:blks){
            System.out.println(b);
        }

        FSDataInputStream in = fs.open(file);
        in.seek(1048676);
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
    }


    @After
    public void close() throws IOException {
        fs.close();
    }
}
