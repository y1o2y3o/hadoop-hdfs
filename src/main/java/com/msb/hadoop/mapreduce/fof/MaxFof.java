package com.msb.hadoop.mapreduce.fof;

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

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;

public class MaxFof {
    static class IntPair implements WritableComparable<IntPair> {
        private IntWritable year;
        private IntWritable temperature;

        public IntPair() {
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

    static class MaxTemperatureMapper extends Mapper<LongWritable, Text, IntPair, Text> {
        private final NcdcRecordParser parser = new NcdcRecordParser();
        private HashMap<String, String> dict = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            final BufferedReader reader = new BufferedReader(new FileReader(new File("dict")));

            String line = reader.readLine();
            while (line != null) {
                final String[] split = line.split("\t");
                //System.out.println(line);
                dict.put(split[0], split[1]);
                line = reader.readLine();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if (parser.isValidTemperature()) {
                context.write(new IntPair(parser.getYearInt(), parser.getAirTemperature()), new Text(parser.getQuality() + "\t" + dict.get(parser.getQuality())));
            }
        }
    }

    static class MaxTemperatureReducer extends Reducer<IntPair, Text, IntPair, Text> {
        @Override
        protected void reduce(IntPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext() && i < 2) {
                Text value = iterator.next();
                ++i;
                context.write(key, value);
            }
        }
    }

    public static class FirstPartitioner extends Partitioner<IntPair, Text> {
        @Override
        public int getPartition(IntPair intPair, Text values, int numPartitions) {
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
        job.setJarByClass(MaxFof.class);
        job.setJar("target/hadoop-hdfs-1.0.jar");

        // 路径
        Path in = new Path("fof/input");
        TextInputFormat.addInputPath(job, in);
        Path out = new Path("fof/output");
        if (out.getFileSystem(conf).exists(out)) {
            out.getFileSystem(conf).delete(out, true);
        }
        TextOutputFormat.setOutputPath(job, out);

        // mapred
        job.addCacheFile(new Path("ncdc/cache/dict").toUri());
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(MaxTemperatureMapper.class);

        job.setPartitionerClass(FirstPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);

        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MaxTemperatureReducer.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntPair.class);
        job.setOutputValueClass(Text.class);

        // run
        job.waitForCompletion(true);
    }

}
