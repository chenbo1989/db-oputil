package chenbo.cimiss.transfer;

import chenbo.cimiss.DBPool;
import chenbo.cimiss.TableMappingManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by chenbo on 2019/3/31.
 */
public class TransferTask implements Runnable {
    private static Logger logger = LogManager.getLogger(TransferTask.class);

    private Connection srcConn, destConn;

    private String selectTemplate;
    private String selectSQL;

    private int fetchBatch = 1000;

    private int batch = 1000;

    private String syncMode = "one";

    private int period = 3600;

    private long startPos = 0;

    private volatile boolean stop = false;

    private TableMappingManager mappingManager;

    public TransferTask(String taskName, Properties properties, String srcPool, String defaultSrcDB, String destPool, String defaultDestDB) {
        mappingManager = new TableMappingManager(taskName, properties, defaultSrcDB, defaultDestDB);

        this.syncMode = properties.getProperty("sync.mode", "one");

        this.period = Integer.parseInt(properties.getProperty("task.period", "3600"));

        logger.info("task["+taskName+"] config: "+
                mappingManager.getSrcDB()+"."+
                mappingManager.getSrcTable()+
                " -> " +
                mappingManager.getDestDB()+
                ":"+
                mappingManager.getDestTable());

        this.srcConn = DBPool.getInstance().getConnection(srcPool);
        this.destConn = DBPool.getInstance().getConnection(destPool);

        this.selectTemplate = properties.getProperty("task.select", "select * from " + mappingManager.getSrcTable());

        this.fetchBatch = Integer.parseInt(properties.getProperty("fetch.batch", "1000"));

        this.batch = Integer.parseInt(properties.getProperty("write.batch", "1000"));
    }

    private FetchStatus transfer(ResultSet resultSet) throws SQLException {
        //获取映射后的字段列表
        //ResultSetMetaData metaData = resultSet.getMetaData();
        //int count = metaData.getColumnCount();

        final Map<String, String> mapping = mappingManager.getFieldsMap();

        //TODO 可以再判断一下 resultSet是否包含需要的列

        //注意字段的对应关系
        final List<String> destFields = mappingManager.getDestFields();
        final String sql = mappingManager.getInsertSQLTemp();

        logger.debug("SQL: " + sql);

        //准备执行插入
        PreparedStatement pstmt = destConn.prepareStatement(sql);

        int rows = 0;

        while (resultSet.next()) {

            //设置当前行数据
            int num = 0;
            for (String destField : destFields) {
                final String srcField = mapping.get(destField);
                final Object value = resultSet.getObject(srcField);
                pstmt.setObject(++num, value);
            }

            //添加到批任务中
            pstmt.addBatch();
            ++rows;

            if (rows % batch == 0) {
                submitBatch(batch, pstmt);
            }
        }

        if (rows == 0) {
            return FetchStatus.NO_MORE_DATA;
        }

        int left = rows % batch;

        if (left > 0) {
            submitBatch(left, pstmt);
        }

        startPos += rows;

        logger.info("Total transferred: " + startPos);

        return FetchStatus.HAS_MORE_DATA;
    }

    /**
     * 提交当前批任务
     * @param expect
     * @param pstmt
     * @throws SQLException
     */
    private void submitBatch(int expect, PreparedStatement pstmt) throws SQLException {
        int[] updates = pstmt.executeBatch();
        pstmt.clearBatch();
        logger.info(expect + " rows submitted");
    }

    /**
     * 执行一次
     * @return
     */
    private FetchStatus runOnce() {
        logger.info("runOnce: "+ selectSQL);
        PreparedStatement stmt = null;
        try {
            FetchStatus status;
            stmt = srcConn.prepareStatement(selectSQL);
            ResultSet rs = stmt.executeQuery();
            if (rs != null) {
                status = transfer(rs);
                rs.close();
            }else{
                status = FetchStatus.EXCEPTION;
            }
            return status;
        } catch (SQLException ex) {
            ex.printStackTrace();
            return FetchStatus.EXCEPTION;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {

                }
            }
        }
    }

    private void sleep(int seconds) {
        try {
            Thread.sleep(seconds * 1000L);
        } catch (InterruptedException ex) {

        }
    }


    public void run() {
        int tryTimes = 0;

        //初始化
        this.stepInit();

        while (!stop) {

            FetchStatus status = runOnce();

            if (status == FetchStatus.EXCEPTION) {
                //执行异常：重试
                tryTimes++;
                if (tryTimes >= 3) {
                    logger.error("Exception for 3 times, terminate task");
                    break;
                } else {
                    logger.warn("Exception executing, retry later...");
                    sleep(60 * tryTimes);
                    continue;
                }
            } else if (status == FetchStatus.NO_MORE_DATA) {
                //当前条件下没有数据
                logger.warn("No more data");
                if ("one".equals(syncMode)) {
                    logger.info("No more data in one sync-mode, terminate task");
                    break;
                }
                startPos = 0;
            } else {
                //递进
                tryTimes = 0;
                this.stepOn();
                continue;
            }

            tryTimes = 0;
            this.stepOn();
            sleep(period);
        }

        if (stop) {
            logger.info("Task terminated by peers");
        }

        cleanUp();
    }

    private void makeSelect() {
        selectSQL = selectTemplate
                .replaceAll("<TABLE>", mappingManager.getSrcTable())
                .replaceAll("<START>", String.valueOf(startPos))
                .replaceAll("<LIMIT>", String.valueOf(fetchBatch));
    }

    private void stepInit(){
        this.makeSelect();
    }

    /**
     * 修改递进条件 如修改ID的最小值、修改时间范围，以便直接或间接影响下一次查询的条件
     */
    private void stepOn() {
        this.makeSelect();
    }

    public void stop() {
        this.stop = true;
    }

    private void cleanUp(){

    }
}
