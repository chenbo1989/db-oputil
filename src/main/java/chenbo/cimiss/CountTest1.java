package chenbo.cimiss;

/**
 * Created by chenbo on 2019/4/17.
 */
public class CountTest1 {
    public static void main(String[] args)  throws Exception{
        CountQuery query = new CountQuery("gbase8a");

        System.out.println(query.count("usr_sod.mv_station_info_4merge"));
    }
}
