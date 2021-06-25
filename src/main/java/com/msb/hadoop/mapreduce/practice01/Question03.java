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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;

// 3) 求各个部门的最早员工入职日期
public class Question03 {

    static class Map03 extends Mapper<LongWritable, Text, Text, Text> {
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
            String deptId = valueArr[7];
            String deptName = dept.get(deptId);
            String date = valueArr[4].replaceFirst("月","");
            context.write(new Text(dept.get(deptId)), new Text(date));
        }
    }

    static class Reducer03 extends Reducer<Text, Text, Text, Text> {
        @lombok.SneakyThrows
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sumSalary = 0;
            DateFormat df = new SimpleDateFormat("dd-MM-yy", Locale.CHINA);
            Date early = new Date(9999, Calendar.JANUARY, 3);
            for (Text t : values) {
                Date d = df.parse(t.toString());
                if(d.compareTo(early) < 0){
                    early = d;
                }
            }
            context.write(key, new Text(early.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "root");
        // 配置
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // setJar
        job.setJar("target/hadoop-hdfs-1.0.jar");
        job.setJarByClass(Question03.class);

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
        job.setMapperClass(Map03.class);
        job.setReducerClass(Reducer03.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.waitForCompletion(true);
    }
}
