package com.msb.hadoop.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class MyWordCount {

    // bin/hadoop command [genericOptions] [commandOptions]
    // hadoop jar ss.jar ooxx -D ooxx=ooxx inpath outpath
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration(true);



        // 让框架知道是windows异构平台运行(unix可以不设)
        conf.set("mapreduce.app-submission.cross-platform", "true");

        // 本地运行?
        conf.set("mapreduce.framework.name", "local");

        GenericOptionsParser parser = new GenericOptionsParser(conf, args); // 工具类帮我们把 -D 的属性set到conf里，会留下程序需要的参数
        String[] othargs = parser.getRemainingArgs();

        Job job = org.apache.hadoop.mapreduce.Job.getInstance(conf);

        //上传jar包
        job.setJar("/Users/zksfromusa/SpringBoot Projects/hadoop-hdfs/target/hadoop-hdfs-1.0.jar");
        job.setJarByClass(MyWordCount.class);

        // Specify various job-specific parameters
        job.setJobName("job_01");

//        job.setInputFormatClass();
//        FileInputFormat.setMinInputSplitSize();

        Path infile = new Path(othargs[0]);
        TextInputFormat.addInputPath(job, infile);

        Path outfile = new Path(othargs[1]);
        if (outfile.getFileSystem(conf).exists(outfile)) {
            outfile.getFileSystem(conf).delete(outfile, true);
        }

        TextOutputFormat.setOutputPath(job, outfile);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);

        job.setNumReduceTasks(2);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
    }
}




