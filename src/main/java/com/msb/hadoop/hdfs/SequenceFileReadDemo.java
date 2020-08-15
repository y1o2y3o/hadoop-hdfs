package com.msb.hadoop.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;


public class SequenceFileReadDemo {

    public static void main(String[] args) throws IOException, InterruptedException {
        // 配置
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://mycluster/"), conf, "root");
        Path path = new Path("seq");

        // open写入的文件
        Path file = new Path(path.toUri()+"/numbers.seq");
        System.out.println(path.toUri());

        // 创建reader
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, file,conf);
        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

        // 读取
        long position = reader.getPosition();
        while(reader.next(key, value)){
            String synSeen = reader.syncSeen() ? "*":"";
            System.out.printf("[%s%s]\t%s\t%s\n", position, synSeen, key, value);
            position = reader.getPosition();
        }

        IOUtils.closeStream(reader);
    }
}
