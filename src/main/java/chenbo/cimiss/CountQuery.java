package chenbo.cimiss;

import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by chenbo on 2019/3/30.
 */
public class CountQuery  extends JDBCBasicQuery {
    public CountQuery(String database) {
        super(database);
    }

    public long count(final String table) throws Exception {
        final String sql = "select count(*) from " + table;
        Statement stmt = conn.createStatement();
        stmt.setQueryTimeout(120);
        ResultSet rst = stmt.executeQuery(sql);
        long ret = 0;
        if (rst != null) {
            rst.setFetchSize(1);
            if (rst.next()) {
                ret = rst.getLong(1);
            }
            rst.close();
        }
        stmt.close();
        return ret;
    }
}
