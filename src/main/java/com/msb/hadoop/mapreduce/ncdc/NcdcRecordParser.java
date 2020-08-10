package com.msb.hadoop.mapreduce.ncdc;

import org.apache.hadoop.io.Text;


public class NcdcRecordParser {
    private static final float MISSING_TEMPERATURE = 9999;

    private String year;
    private float airTemperature;
    public String quality;

    public void parse(String record){
        quality = record.substring(32, 33);
        if(isValidTemperature()){
            year = record.substring(14,18);
            airTemperature = Float.parseFloat(record.substring(25, 29));
        }

    }

    public void parse(Text record){
        parse(record.toString());
    }

    public boolean isValidTemperature(){
        return airTemperature != MISSING_TEMPERATURE
                && quality.matches("[0-9]");
    }

    public String getYear() {
        return year;
    }

    public float getAirTemperature() {
        return airTemperature;
    }
}
