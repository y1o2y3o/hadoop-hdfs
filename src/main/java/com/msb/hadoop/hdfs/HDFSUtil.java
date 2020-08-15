package com.msb.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class HDFSUtil {
    private static Configuration conf;
    private static FileSystem fs;

    static {
        try {
            Configuration conf = new Configuration();
            fs = FileSystem.get(URI.create("hdfs://mycluster/"),conf, "root");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Configuration getConf(){
        return conf;
    }

    public FileSystem getFs(){
        return fs;
    }

    public static FSDataInputStream open(String uri) throws IOException {
        Path path = new Path(uri);
        return fs.open(path);
    }
}
