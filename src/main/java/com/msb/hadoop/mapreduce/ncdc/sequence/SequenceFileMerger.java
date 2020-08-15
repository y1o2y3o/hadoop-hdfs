package com.msb.hadoop.mapreduce.ncdc.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class SequenceFileMerger {
    static class  SeqFileMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), value);
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 配置
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "seq_job1");

        // 传jar包
        job.setJarByClass(SequenceFileMerger.class);
        job.setJar("target/hadoop-hdfs-1.0.jar");

        // 路径
        Path infile = new Path("ncdc/input");
        Path outfile = new Path("ncdc/output");
        if(outfile.getFileSystem(conf).exists(outfile)){
            outfile.getFileSystem(conf).delete(outfile, true);
        }
        TextInputFormat.addInputPath(job, infile);
        SequenceFileOutputFormat.setOutputPath(job, outfile);

        // mapred
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(SeqFileMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(1);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // run
        job.waitForCompletion(true);
    }
}
