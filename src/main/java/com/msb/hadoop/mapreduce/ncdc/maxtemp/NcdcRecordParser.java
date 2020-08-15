package com.msb.hadoop.mapreduce.ncdc.maxtemp;

import org.apache.hadoop.io.Text;

import java.io.*;


public class NcdcRecordParser {
    private static final int MISSING_TEMPERATURE = 9999;

    private String year;
    private int yearInt;
    private int airTemperature;
    public String quality;

    public void parse(String record) {
        year = record.substring(15, 19);
        String airTemperatureString;
        try{
            if (record.charAt(87) == '+') {
                airTemperatureString = record.substring(88, 92);
            } else {
                airTemperatureString = record.substring(87, 92);
            }
            airTemperature = Integer.parseInt(airTemperatureString);
            yearInt = Integer.parseInt(year);
            quality = record.substring(92, 93);
        } catch (Exception e){
            System.err.print(e.toString());
            quality = "bad";
        }
    }


    public void parse(Text record) {
        parse(record.toString());
    }

    public boolean isValidTemperature() {
        return airTemperature != MISSING_TEMPERATURE
                && quality.matches("[01459]");
    }

    public String getYear() {
        return year;
    }

    public int getYearInt(){
        return yearInt;
    }


    public int getAirTemperature() {
        return airTemperature;
    }

    public String getQuality() {
        return quality;
    }

    public static void main(String[] args) throws IOException {
        NcdcRecordParser parser = new NcdcRecordParser();

        // read file
        File in = new File("src/main/resources/010015-99999-2010");
        FileReader fileReader = new FileReader(in);
        BufferedReader reader = new BufferedReader(fileReader);

        // write file
        File out = new File( "src/main/resources/010015-99999-2010.out");
        FileWriter fileWriter = new FileWriter(out);
        BufferedWriter writer = new BufferedWriter(fileWriter);

        String record;
        while ((record = reader.readLine()) != null) {
            parser.parse(record);
            writer.newLine();
            writer.write(String.format("%s, %d, %s", parser.getYear(), parser.getAirTemperature(), parser.getQuality()));
        }

        writer.close();
        reader.close();


    }
}
