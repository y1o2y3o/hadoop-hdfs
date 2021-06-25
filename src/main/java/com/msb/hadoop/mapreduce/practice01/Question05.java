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
import java.util.ArrayList;
import java.util.HashMap;

// 5) 列出工资比上司高的员工姓名及其工资
public class Question05 {

    static class Map05 extends Mapper<LongWritable, Text, Text, Text> {
        private HashMap<String, String> dept = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            File file = new File("dept");
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = null;
            while ((line = reader.readLine()) != null) {
                dept.put(line.split(",")[0], line);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] valueArr = value.toString().split(",");
            String empId = valueArr[0];
            String managerId = valueArr[3];
            String salary = valueArr[5];
            String empName = valueArr[1];
            context.write(new Text(empId), new Text(String.format("M,%s,%s",empName, salary)));
            context.write(new Text(managerId), new Text(String.format("E,%s,%s",empName, salary)));
        }
    }

    static class Reducer05 extends Reducer<Text, Text, Text, DoubleWritable> {


        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> arrayList = new ArrayList<>();
            double managerSalary = Double.MAX_VALUE;
            for (Text t : values) {
                String role = t.toString().split(",")[0];
                if(role.equals("M")){

                    managerSalary = Double.parseDouble(t.toString().split(",")[2]);
                    break;
                } else {
                    arrayList.add(t.toString());
                }
            }

            for (String t : arrayList){
                String[] tArr = t.toString().split(",");
                String name = tArr[1];
                double salary = Double.parseDouble(tArr[2]);
                if(salary > managerSalary){
                    context.write(new Text(name), new DoubleWritable(salary));
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "root");
        // 配置
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // setJar
        job.setJar("target/hadoop-hdfs-1.0.jar");
        job.setJarByClass(Question05.class);

        // 分布式缓存
        job.addCacheFile(new Path("practice01/cache/dept").toUri());

        // in out
        TextInputFormat.addInputPath(job, new Path("practice01/input"));

        Path outfile = new Path("practice01/output");
        if (outfile.getFileSystem(conf).exists(outfile)) {
            outfile.getFileSystem(conf).delete(outfile, true);
        }
        TextOutputFormat.setOutputPath(job, outfile);

        // mapred
        job.setMapperClass(Map05.class);
        job.setReducerClass(Reducer05.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.waitForCompletion(true);
    }
}
