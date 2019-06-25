package chenbo.cimiss;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by chenbo on 2019/6/19.
 */

public class TestQuery {
    private Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private JDBCBasicQuery query;

    @Before
    public void setUp(){
        query = new JDBCBasicQuery("mysql-local");
    }

    @Test
    public void testQ1() throws Exception {
        String sql = "select * from test1";
        System.out.println(gson.toJson(query.query(sql)));
    }

    @Test
    public void testQ2() throws Exception{
        String sql = "select * from test2";
        System.out.println(gson.toJson(query.query(sql)));
    }

    @Test
    public void testQ3() throws Exception{
        String sql = "select * from test3";
        System.out.println(gson.toJson(query.query(sql)));
    }

    @Test
    public void testShowCreate() throws Exception{
        String sql = "show create table test1";
        System.out.println(gson.toJson(query.query(sql)));
    }

    @Test
    public void testShowTables() throws Exception{
        String sql = "show tables in cimiss_test";
        System.out.println(gson.toJson(query.query(sql)));
    }
}
