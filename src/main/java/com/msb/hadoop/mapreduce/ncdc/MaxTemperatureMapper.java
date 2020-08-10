package com.msb.hadoop.mapreduce.ncdc;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private final Text year = new Text();
    private final FloatWritable airTemperature = new FloatWritable();
    private final NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        if(parser.isValidTemperature()){
            year.set(parser.getYear());
            airTemperature.set(parser.getAirTemperature());
            context.write(year, airTemperature);
        }
    }
}
