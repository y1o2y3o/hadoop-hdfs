import com.msb.hadoop.mapreduce.ncdc.NcdcRecordParser;
import org.junit.Test;

public class TT {
    @Test
    public void test(){
        String record = "030050 99999  19291001    45.3  4    40.0  4  1001.6  4  9999.9  0   17.1  4    4.5  4    8.9  999.9    51.1    44.1*  0.00I 999.9  000000";
        String record2="STN--- WBAN   YEARMODA    TEMP       DEWP      SLP        STP       VISIB      WDSP     MXSPD   GUST    MAX     MIN   PRCP   SNDP   FRSHTT";

        NcdcRecordParser parser = new NcdcRecordParser();
        parser.parse(record2);
        System.out.println(parser.getYear());
        System.out.println(parser.getAirTemperature());
        System.out.println(parser.quality);
        System.out.println(parser.isValidTemperature());
    }
}
