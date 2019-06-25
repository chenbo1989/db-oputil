package chenbo.cimiss.transfer;

import chenbo.cimiss.DBPool;
import cn.golaxy.gkg.base.ROOT;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by chenbo on 2019/4/1.
 */
public class Migrate8a {
    private static Logger logger = LogManager.getLogger(Migrate8a.class);

    private Connection from, to;
    private int fetchSize = 1000;
    private int batch = 1000;

    private ExecutorService service;

    public Migrate8a(String dbFrom, String dbTo) {
        from = DBPool.getInstance().getConnection(dbFrom);
        to = DBPool.getInstance().getConnection(dbTo);

        service = Executors.newFixedThreadPool(30);
    }

    static class WriteJob implements Runnable {
        private PreparedStatement statement;

        public WriteJob(PreparedStatement statement) {
            this.statement = statement;
        }

        @Override
        public void run() {
            try {
                statement.executeBatch();
                statement.close();
            } catch (Exception ex) {
                System.err.println(ex.getMessage());
            }
        }
    }

    public long migrate(String table, String where, String limit) throws Exception {
        final String sql = "select * from " + table +" "+ where+" "+limit;
        PreparedStatement stmtFrom = from.prepareStatement(sql);
        logger.info(sql);
        stmtFrom.setFetchSize(fetchSize);
        ResultSet rs = stmtFrom.executeQuery();
        final int cols = rs.getMetaData().getColumnCount();
        final String INSERT = "insert into 'user_sod'." + table + " values " + makeValuesPlaceholder(cols);
        logger.debug(INSERT);
        PreparedStatement stmtTo = to.prepareStatement(INSERT);
        long num = 0;
        while (rs.next()) {
            for (int i = 1; i <= cols; ++i) {
                Object value = rs.getObject(i);
                stmtTo.setObject(i, value);
            }
            stmtTo.addBatch();

            ++num;
            if (num % batch == 0) {
                service.submit(new WriteJob(stmtTo));
                stmtTo = to.prepareStatement(INSERT);
                logger.info(num + " rows transferred");
            }
        }
        if (num % batch != 0) {
            service.submit(new WriteJob(stmtTo));
            logger.info(num + " rows transferred");
        }
        rs.close();
        stmtTo.close();

        return num;
    }

    public long count(String table, String where) throws SQLException {
        final String sql = "select count(*) from " + table + " " + where;
        ResultSet rs = from.createStatement().executeQuery(sql);
        long n = 0;
        if(rs.next()) {
            n = rs.getLong(1);
        }
        rs.close();
        return n;
    }

    private String makeValuesPlaceholder(int n) {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < n; ++i) {
            sb.append("?");
            if (i < n - 1) {
                sb.append(",");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public int getBatch() {
        return batch;
    }

    public void setBatch(int batch) {
        this.batch = batch;
    }

    public static void main(String[] args) throws Exception {
        if(args.length<3){
            System.err.println("java Migrate8a <table> <start-date> <end-date>");
            System.exit(1);
        }

        Properties config = ROOT.getProperties("migrate.properties");

        Migrate8a job = new Migrate8a(config.getProperty("from", "gbase8a-old"),
                config.getProperty("to", "gbase8t"));
        job.setFetchSize(Integer.parseInt(config.getProperty("fetchSize", "10000")));
        job.setBatch(Integer.parseInt(config.getProperty("batch", "10000")));

        long limit = Integer.parseInt(config.getProperty("limit", "100000"));

        final String table = args[0];
        String startDate = args[1];
        final String finalDate = args[2];

        long total = 0;

        while(startDate.compareTo(finalDate)<0) {

            String endDate = SQLUtil.addDate(startDate, 1);
            final String where = "where d_datetime>='" + startDate + "' and d_datetime<'" + endDate + "'";
            System.out.println("Run[" + where + "] " + new Date());

            long regionTotal = job.count(table, where);

            long start = 0;
            while (true) {
                final String sqlLimit = "limit " + start + "," + limit;
                System.out.println(sqlLimit + "/" + regionTotal);

                long num = job.migrate(table, where, sqlLimit);
                if (num == 0) {
                    break;
                }
                start += num;
                total += num;
                System.out.println("Row count: " + start + "/" + regionTotal + "/" + total);
            }

            startDate = endDate;
        }

        System.out.println("Total rows: "+ total);

        //
        //job.migrate("surf_wea_chn_mul_day_tab");
//        job.migrate("surf_wea_chn_mul_hor_tab");
//        job.migrate("surf_wea_chn_mul_min_tab");
        //job.migrate("surf_wea_chn_pre_min_tab", "10100, 50000");
//        job.migrate("surf_wea_chn_pre_min_accu_tab");
    }
}
