package com.hadoop.work;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class MyFSDataInputStream {
    private final InputStreamReader inputStreamReader;
    private final BufferedReader bufferedReader;

    public MyFSDataInputStream(InputStream in) {
        inputStreamReader = new InputStreamReader(in);
        bufferedReader = new BufferedReader(inputStreamReader);
    }

    public String readLine() throws IOException {
        return bufferedReader.readLine();
    }

    public void close() throws IOException {
        bufferedReader.close();
        inputStreamReader.close();
    }
}
