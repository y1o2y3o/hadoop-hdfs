package com.msb.hadoop.mapreduce.ncdc.sort;

import com.msb.hadoop.mapreduce.ncdc.maxtemp.NcdcRecordParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

public class MaxTemperatureUsingSecondarySort {
    static class IntPair implements WritableComparable<IntPair> {
        private IntWritable year;
        private IntWritable temperature;

        public IntPair(){
            year = new IntWritable();
            temperature = new IntWritable();
        }

        public IntPair(int year, int temperature) {
            this.year = new IntWritable(year);
            this.temperature = new IntWritable(temperature);
        }

        public IntPair(IntWritable year, IntWritable temperature) {
            this.year = year;
            this.temperature = temperature;
        }

        @Override
        public int compareTo(IntPair o) {
            int res = year.compareTo(o.year);
            if (res != 0)
                return res;
            return -temperature.compareTo(o.temperature);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            year.write(out);
            temperature.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            year.readFields(in);
            temperature.readFields(in);
        }

        @Override
        public int hashCode() {
            return year.hashCode() * 163 + temperature.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof IntPair) {
                IntPair ip = (IntPair) obj;
                return year.equals(ip.year) && temperature.equals(ip.temperature);
            }
            return false;
        }

        public IntWritable getYear() {
            return year;
        }

        public void setYear(IntWritable year) {
            this.year = year;
        }

        public IntWritable getTemperature() {
            return temperature;
        }

        public void setTemperature(IntWritable temperature) {
            this.temperature = temperature;
        }

        @Override
        public String toString() {
            return year.toString() + "\t" + temperature.toString();
        }
    }

    static class MaxTemperatureMapper extends Mapper<LongWritable, Text, IntPair, NullWritable> {
        private final NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if (parser.isValidTemperature()) {
                context.write(new IntPair(parser.getYearInt(), parser.getAirTemperature()), NullWritable.get());
            }
        }
    }

    static class MaxTemperatureReducer extends Reducer<IntPair, NullWritable, IntPair, NullWritable> {
        @Override
        protected void reduce(IntPair key, Iterable <NullWritable> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            Iterator<NullWritable> iterator = values.iterator();
            while(iterator.hasNext() && i < 2){
                iterator.next();
                ++i;
                context.write(key, NullWritable.get());
            }
        }
    }

    public static class FirstPartitioner extends Partitioner<IntPair, NullWritable> {
        @Override
        public int getPartition(IntPair intPair, NullWritable nullWritable, int numPartitions) {
            return Math.abs(intPair.getYear().get() * 127) % numPartitions;
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(IntPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntPair ip1 = (IntPair) a;
            IntPair ip2 = (IntPair) b;
            int cmp = ip1.getYear().compareTo(ip2.getYear());
            if (cmp != 0) {
                return cmp;
            }
            return -ip1.getTemperature().compareTo(ip2.getTemperature());
        }
    }

    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(IntPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntPair ip1 = (IntPair) a;
            IntPair ip2 = (IntPair) b;
            return ip1.getYear().compareTo(ip2.getYear());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 配置
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // jar包
        job.setJarByClass(MaxTemperatureUsingSecondarySort.class);
        job.setJar("target/hadoop-hdfs-1.0.jar");

        // 路径
        Path in = new Path("ncdc/input");
        TextInputFormat.addInputPath(job, in);
        Path out = new Path("ncdc/output4");
        if(out.getFileSystem(conf).exists(out)){
            out.getFileSystem(conf).delete(out, true);
        }
        TextOutputFormat.setOutputPath(job, out);

        // mapred
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(MaxTemperatureMapper.class);

        job.setPartitionerClass(FirstPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);

        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(MaxTemperatureReducer.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntPair.class);
        job.setOutputValueClass(NullWritable.class);

        // run
        job.waitForCompletion(true);
    }

}
