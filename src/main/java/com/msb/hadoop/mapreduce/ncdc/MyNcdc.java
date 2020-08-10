package com.msb.hadoop.mapreduce.ncdc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class MyNcdc {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration(true);
        Job job = Job.getInstance(conf);

        job.setJarByClass(MyNcdc.class);

        // Specify various job-specific parameters
        job.setJobName("job_ncdc_01");

        Path infile = new Path("/data/ncdc/input");
        TextInputFormat.addInputPath(job, infile);

        Path outfile = new Path("/data/ncdc/output");
        if (outfile.getFileSystem(conf).exists(outfile)) {
            outfile.getFileSystem(conf).delete(outfile, true);
        }

        TextOutputFormat.setOutputPath(job, outfile);
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
//        job.setReducerClass(MyReducer.class);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);

    }
}




