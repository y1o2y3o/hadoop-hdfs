package com.msb.hadoop.mapreduce.ncdc.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SequenceFileReaderDemo {

    public static void main(String[] args) throws IOException, InterruptedException {

        // 配置
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();

        // 路径
        Path path = new Path("ncdc/output/part-r-00000");

        // key value
        NullWritable key = NullWritable.get();
        Text value = new Text();

        // SequenceFile
        SequenceFile.Reader.Option fileOption = SequenceFile.Reader.file(path);
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, fileOption);
        int i = 0;
        while(reader.next(key, value) && i < 100){
            System.out.println(value.toString());
            ++i;
        }

    }
}
