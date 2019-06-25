package chenbo.cimiss.transfer;

/**
 * Created by chenbo on 2019/4/2.
 */
public class SQLUtil {
    public static String addDate(String startDate, int months) {
        String[] parts = startDate.split("-");
        int year = Integer.parseInt(parts[0]), month = Integer.parseInt(parts[1]);
        int m = month + months - 1;
        return String.format("%04d-%02d-%s", year + m / 12, m % 12 + 1, parts[2]);
    }

    public static void main(String[] args) {
        System.out.println(addDate("2017-12-01", 1));
    }
}
