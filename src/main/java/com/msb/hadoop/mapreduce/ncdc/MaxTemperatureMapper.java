package com.msb.hadoop.mapreduce.ncdc;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private final Text year = new Text();
    private final FloatWritable airTemperature = new FloatWritable();
    private final NcdcRecordParser parser = new NcdcRecordParser();
    private static final Log LOG = LogFactory.getLog(MaxTemperatureMapper.class);

    enum Temperature{
        OVER_100
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        if(parser.isValidTemperature()){
            System.out.println("Map key: "+key);
            LOG.info("Map Key: "+key);
            float airTemp = parser.getAirTemperature();
            if(airTemp > 100){
                System.err.println("Temperature over 80 degrees for input : "+value);
                context.setStatus("Detected possibly corrupt record: see logs");
                context.getCounter(Temperature.OVER_100).increment(1);
            }
            year.set(parser.getYear());
            airTemperature.set(parser.getAirTemperature());
            context.write(year, airTemperature);
        }
    }
}
