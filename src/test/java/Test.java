import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.URI;

public class Test {
    public Configuration conf = null;
    public FileSystem fs = null;

    @Before
    public void conn() throws IOException, InterruptedException {
        conf = new Configuration(true);
        fs = FileSystem.get(URI.create("hdfs://mycluster/"),conf, "god");
    }



    @org.junit.Test
    public void test() throws IOException {
        Configuration conf = new Configuration();
        conf.addResource("data/configuration-1.xml");
        conf.set("color","pink");
//        for(Map.Entry<String, String> entry: conf){
//            System.out.println(entry.getKey()+","+entry.getValue());
//        }
        System.out.println(conf.get("color"));
    }


    @After
    public void close() throws IOException {
        fs.close();
    }
}
