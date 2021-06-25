package com.msb.hadoop.hdfs;

import com.hadoop.work.MyFSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URLStreamHandler;

public class TestHDFS {

    public Configuration conf = null;
    public FileSystem fs = null;

    @Before
    public void conn() throws IOException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "root");
        conf = new Configuration(true);
        fs = FileSystem.get(URI.create("hdfs://mycluster/"), conf, "root");
    }

    @Test
    public void mkdir() throws IOException {
        Path dir = new Path("zks001");
        if (fs.exists(dir)) {
            fs.delete(dir, true);
        }
        fs.mkdirs(dir);
    }

    @Test
    public void upload() throws IOException {
        BufferedInputStream input = new BufferedInputStream(new FileInputStream(new File("./data/hello.txt")));
        Path outfile = new Path("zks01/out01.txt");
        FSDataOutputStream output = fs.create(outfile);
        IOUtils.copyBytes(input, output, conf, true);
    }

    @Test
    public void download() throws IOException {

        Path inFile = new Path("zks01/out01.txt");
        FSDataInputStream input = fs.open(inFile);
        BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream("./down.txt"));
        IOUtils.copyBytes(input, output, conf, true);
    }


    @Test
    public void blocks() throws IOException {
        Path file = new Path("/user/god/data.txt");
        FileStatus fss = fs.getFileStatus(file);
        BlockLocation[] blks = fs.getFileBlockLocations(fss, 0, fss.getLen());
        for (BlockLocation b : blks) {
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

    @Test
    public void readLine() throws IOException {
        Path inFile = new Path("/user/root/ncdc/testinput/testinput");
        MyFSDataInputStream input = new MyFSDataInputStream(fs.open(inFile));
        String line;
        while ((line = input.readLine()) != null) {
            System.out.println("READ: " + line);
        }
        input.close();
    }

    @Test
    public void output() throws IOException {
        FsUrlStreamHandlerFactory fsUrlStreamHandlerFactory = new FsUrlStreamHandlerFactory();
        URLStreamHandler urlStreamHandler = fsUrlStreamHandlerFactory.createURLStreamHandler("");

        Path inFile = new Path("/user/root/ncdc/testinput/testinput");
        MyFSDataInputStream input = new MyFSDataInputStream(fs.open(inFile));
        String line;
        while ((line = input.readLine()) != null) {
            System.out.println("READ: " + line);
        }
        input.close();
    }
}
