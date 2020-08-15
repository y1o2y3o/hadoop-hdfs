package com.msb.hadoop.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;


public class SequenceFileWriteDemo {
    private static final String[] DATA = {
            "One, two, buckle my shoe1",
            "Three, four, buckle my shoe2",
            "Five, six, buckle my shoe3",
            "Seven, eight, buckle my shoe4",
            "Nine, tem, buckle my shoe5",
    };

    public static void main(String[] args) throws IOException, InterruptedException {
        // 配置
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://mycluster/"), conf, "root");
        Path path = new Path("seq");

        // 创建文件夹路径
        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }

        // 创建写入的文件
        Path file = new Path(path.toUri()+"/numbers.seq");
        System.out.println(path.toUri());

        // 创建writer
        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, file, key.getClass(), value.getClass());

        // 写入
        for (int i = 0; i < 100; i++) {
            key.set(100 - i);
            value.set(DATA[i % DATA.length]);
            writer.append(key, value);
        }

        IOUtils.closeStream(writer);


    }
}
