package chenbo.cimiss;

public class DataCounter
{
    public static final String USERSPACE = "user_sod";

    public static final String[] tables = {
            "surf_wea_chn_pre_min_tab", "surf_wea_chn_pre_min_accu_tab", "surf_wea_chn_mul_min_tab", "surf_wea_chn_mul_hor_tab"
    };

    private static void printHeader() {
        System.out.print("database\t");
        for (String table : tables) {
            System.out.print(table + "\t");
        }
        System.out.println();
    }

    public static void count(String dbname, boolean withUserSpace) throws Exception {
        System.out.print(dbname + "\t");
        CountQuery query = new CountQuery(dbname);
        if (withUserSpace) {
            for (String table : tables) {
                System.out.print(query.count("'" + USERSPACE + "'." + table));
                System.out.print("\t");
            }
        } else {
            for (String table : tables) {
                System.out.print(query.count(table));
                System.out.print("\t");
            }
        }
        System.out.println();
    }

    public static void main( String[] args) throws Exception {

        printHeader();

        count("gbase8t", true);

        count("gbase8a", false);
    }
}
