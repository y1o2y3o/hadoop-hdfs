package com.msb.hadoop.mapreduce.ncdc;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTemperatureReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float maxValue = Float.MIN_VALUE;
        for(FloatWritable value: values){
            maxValue = Math.max(maxValue, value.get());
        }
        context.write(key, new FloatWritable(maxValue));
    }
}
