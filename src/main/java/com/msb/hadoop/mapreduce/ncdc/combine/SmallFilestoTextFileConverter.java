package com.msb.hadoop.mapreduce.ncdc.combine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class SmallFilestoTextFileConverter{

    static class SequenceFileMapper extends Mapper<NullWritable, Text, NullWritable, Text> {
        @Override
        protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), value);
        }
    }

    static class SequenceFileReducer extends Reducer<NullWritable, Text, NullWritable, Text>{
        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text t: values){
                context.write(key, t);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration(true);

        Job job = Job.getInstance(conf);

        Path infile = new Path("ncdc/input");
        WholeFileInputFormat.addInputPath(job, infile);

        Path outfile = new Path("ncdc/output");
        if (outfile.getFileSystem(conf).exists(outfile)) {
            outfile.getFileSystem(conf).delete(outfile, true);
        }

        TextOutputFormat.setOutputPath(job, outfile);

        job.setJarByClass(SmallFilestoTextFileConverter.class);
        job.setJar("/Users/zksfromusa/SpringBoot Projects/hadoop-hdfs/target/hadoop-hdfs-1.0.jar");

        job.setInputFormatClass(WholeFileInputFormat.class);


        job.setMapperClass(SequenceFileMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(SequenceFileReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputFormatClass(TextOutputFormat.class);

        job.waitForCompletion(true);
    }
}
