package com.msb.hadoop.local;

import com.msb.hadoop.mapreduce.ncdc.NcdcRecordParser;

import java.io.*;

public class LocalMapReduceTest {
    private  static final NcdcRecordParser parser = new NcdcRecordParser();

    public static void main(String[] args) throws IOException {
        File infile = new File(args[0]);
        FileInputStream fileInputStream = new FileInputStream(infile);
        InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

        File outfile = new File(args[1]);
        FileOutputStream fileOutputStream = new FileOutputStream(outfile);
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream);
        BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter);

        String line;
        while((line=bufferedReader.readLine())!=null){
            parser.parse(line);
            if(parser.isValidTemperature()){
                bufferedWriter.write(parser.getYear() +"\t" +String.valueOf(parser.getAirTemperature()));
                bufferedWriter.newLine();
            }
        }

        bufferedReader.close();
        bufferedWriter.close();
    }

}
