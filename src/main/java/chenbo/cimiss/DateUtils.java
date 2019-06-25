package chenbo.cimiss;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by chenbo on 2019/6/20.
 */
public class DateUtils {

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd");
    private static SimpleDateFormat datetimeFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
    private static SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");

    public static String formatDate(Date dt){
        if(dt==null){
            return "NULL";
        }
        return dateFormat.format(dt);
    }

    public static String formatDatetime(Date dt){
        if(dt==null) return "NULL";
        return datetimeFormat.format(dt);
    }

    public static String formatTime(Date dt){
        if(dt==null) return "NULL";
        return timeFormat.format(dt);
    }


    public static void main(String[] args) {
        System.out.println(formatDate(new Date()));
        System.out.println(formatDatetime(new Date()));
    }
}
