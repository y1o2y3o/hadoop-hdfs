package com.msb.hadoop.mapreduce.ncdc.maxtemp;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Text year = new Text();
    private final IntWritable airTemperature = new IntWritable();
    private final NcdcRecordParser parser = new NcdcRecordParser();

    enum Temperature{
        OVER_100
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        if(parser.isValidTemperature()){
            int airTemp = parser.getAirTemperature();
            if(airTemp > 1000){
                System.err.println("Temperature over 80 degrees for input : "+value);
                context.setStatus("Detected possibly corrupt record: see logs");
                context.getCounter(Temperature.OVER_100).increment(1);
            }
            year.set(parser.getYear());
            airTemperature.set(parser.getAirTemperature());
            context.write(year, airTemperature);
        }
        context.getCounter("TemperatureQuality", parser.getQuality()).increment(1);
    }
}
