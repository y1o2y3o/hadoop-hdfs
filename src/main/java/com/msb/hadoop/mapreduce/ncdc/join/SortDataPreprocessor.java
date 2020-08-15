package com.msb.hadoop.mapreduce.ncdc.join;

import com.msb.hadoop.mapreduce.ncdc.maxtemp.NcdcRecordParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class SortDataPreprocessor {
    static class CleanerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if(parser.isValidTemperature()){
                context.write(new IntWritable(parser.getAirTemperature()), value);
            }
        }

        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            System.setProperty("HADOOP_USER_NAME", "root");
            Configuration conf = new Configuration(true);
            Job job = Job.getInstance(conf);

            job.setJarByClass(SortDataPreprocessor.class);
            job.setJar("/Users/zksfromusa/SpringBoot Projects/hadoop-hdfs/target/hadoop-hdfs-1.0.jar");


            Path infile = new Path("ncdc/input");
            TextInputFormat.addInputPath(job, infile);

            Path outfile = new Path("ncdc/output");
            if (outfile.getFileSystem(conf).exists(outfile)) {
                outfile.getFileSystem(conf).delete(outfile, true);
            }

            SequenceFileOutputFormat.setOutputPath(job, outfile);

            job.setMapperClass(CleanerMapper.class);
            job.setNumReduceTasks(0);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setCompressOutput(job, true);
            SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

            job.waitForCompletion(true);
        }
    }
}
