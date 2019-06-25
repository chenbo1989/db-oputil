package chenbo.cimiss.mysqlbinlog2;

import chenbo.cimiss.DBPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by chenbo on 2019/6/23.
 */
public class BatchCommit {
    private static Logger LOG = LogManager.getLogger(BatchCommit.class);

    private Connection connection;

    private Map<String, PreparedStatement> buffer = new HashMap<>(8);
    private Map<String, Integer> batchCounter = new ConcurrentHashMap<>(8);

    private final int BATCH_NUM = 100;
    private final int MAX_DELAY = 5;


    public BatchCommit(String dbpool) {
        this.connection = DBPool.getInstance().getConnection(dbpool);
        new Thread(new TimedChecker()).start();
    }

    public synchronized PreparedStatement newStatement(String sql) throws SQLException{
        String type = sql.substring(0, 6);
        if(buffer.containsKey(type)){
            batchCounter.put(type, batchCounter.get(type)+1);
            return buffer.get(type);
        } else {
            batchCounter.put(type, 1);
            PreparedStatement st = connection.prepareStatement(sql);
            buffer.put(type, st);
            return st;
        }
    }

    public synchronized void commit(String sql, PreparedStatement pstmt) throws SQLException {
        String type = sql.substring(0, 6);
        pstmt.addBatch();
        if (batchCounter.get(type) >= BATCH_NUM) {
            LOG.info("counter submit for " + type);
            submit(type, pstmt);
        }
    }

    private synchronized void submit(String type, PreparedStatement pstmt){
        int num = batchCounter.get(type);
        try {
            pstmt.executeBatch();
            pstmt.clearBatch();
            batchCounter.put(type, 0);
            LOG.info(num+" statements submitted");
        }catch (SQLException ex){
            ex.printStackTrace();
        }
    }

    private class TimedChecker implements Runnable{

        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(1000L * MAX_DELAY);
                } catch (Exception ex) {

                }
                final List<String> types = new ArrayList<>(batchCounter.keySet());

                for (String type : types) {
                    if (batchCounter.get(type) > 0) {
                        LOG.info("timer submit for "+type);
                        submit(type, buffer.get(type));
                    }
                }
            }
        }
    }
}
