import org.junit.Test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class RandomTest {
    @Test
    public void test() throws ParseException {
        DateFormat df = new SimpleDateFormat("dd-MM-yy", Locale.CHINA);
        Date date = df.parse("11-4-99");
        System.out.println(date);
    }
}
