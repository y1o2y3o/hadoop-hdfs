package com.msb.hadoop.mapreduce.practice01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

// 1) 求各个部门的总工资
public class Question01 {

    static class Map01 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private HashMap<String, String> dept = new HashMap<>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            File file = new File("dept");
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = null;
            while((line = reader.readLine())!=null){
                dept.put(line.split(",")[0], line);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] valueArr = value.toString().split(",");
            String deptId = valueArr[7];
            String deptName = dept.get(deptId);
            double salary = Double.parseDouble(valueArr[5]);
            context.write(new Text(dept.get(deptId)), new DoubleWritable(salary));
        }
    }

    static class Reducer01 extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sumSalary = 0;
            for(DoubleWritable d: values){
                sumSalary += d.get();
            }
            context.write(key, new DoubleWritable(sumSalary));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "root");
        // 配置
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // setJar
        job.setJar("target/hadoop-hdfs-1.0.jar");
        job.setJarByClass(Question01.class);

        // 分布式缓存
        job.addCacheFile(new Path("practice01/cache/dept").toUri());

        // in out
        TextInputFormat.addInputPath(job, new Path("practice01/input"));

        Path outfile = new Path("practice01/output");
        if(outfile.getFileSystem(conf).exists(outfile)){
            outfile.getFileSystem(conf).delete(outfile,true);
        }
        TextOutputFormat.setOutputPath(job, outfile);

        // mapred
        job.setMapperClass(Map01.class);
        job.setReducerClass(Reducer01.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.waitForCompletion(true);
    }
}
