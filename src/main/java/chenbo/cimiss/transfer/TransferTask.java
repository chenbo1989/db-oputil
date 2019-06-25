package chenbo.cimiss.transfer;

import chenbo.cimiss.DBPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.*;

/**
 * Created by chenbo on 2019/3/31.
 */
public class TransferTask implements Runnable {
    private static Logger logger = LogManager.getLogger(TransferTask.class);

    private Connection srcConn, destConn;

    private String srcTable, destTable;

    private String selectTemplate;
    private String selectSQL;

    private int fetchBatch = 1000;

    private int batch = 1000;

    private String syncMode = "one";

    private int period = 3600;

    private long startPos = 0;

    private Map<String, String> fieldsMap = new HashMap<>();

    private volatile boolean stop = false;

    public TransferTask(String taskName, Properties properties, String srcDB, String destDB) {
        srcDB = properties.getProperty("db.src", srcDB);
        destDB = properties.getProperty("db.dest", destDB);

        String table = properties.getProperty("table", taskName);
        this.srcTable = properties.getProperty("table.src", table);
        this.destTable = properties.getProperty("table.dest", this.srcTable);

        this.syncMode = properties.getProperty("sync.mode", "one");

        this.period = Integer.parseInt(properties.getProperty("task.period", "3600"));

        logger.info("task["+taskName+"] config: "+ srcDB+":"+srcTable+" -> " + destDB+":"+destTable);

        //mapping
        String mapping = properties.getProperty("fields.mapping");
        for (String part : mapping.split("[,;\t]+")) {
            String from, to;
            int pos = part.indexOf(':');
            if (pos < 0) {//不改变迁移后的字段名称
                from = part.trim();
                to = from;
            } else {
                from = part.substring(0, pos).trim();
                to = part.substring(pos + 1).trim();
                if (to.isEmpty()) {//不改变迁移后的字段名称
                    to = from;
                }
            }
            if (from.isEmpty()) {
                continue;
            }
            fieldsMap.put(from.toLowerCase(), to.toLowerCase());
        }

        srcConn = DBPool.getInstance().getConnection(srcDB);
        destConn = DBPool.getInstance().getConnection(destDB);

        selectTemplate = properties.getProperty("task.select", "select * from " + srcTable);

        this.fetchBatch = Integer.parseInt(properties.getProperty("fetch.batch", "1000"));

    }

    private FetchStatus transfer(ResultSet resultSet) throws SQLException {
        //获取映射后的字段列表
        ResultSetMetaData metaData = resultSet.getMetaData();
        int count = metaData.getColumnCount();
        List<String> destFields = new ArrayList<>(count); //新字段名
        Map<String, Integer> indexMap = new HashMap<>(count);//字段名与其下标的对应：原表字名 -> 新表的下标
        for (int i = 0; i < count; ++i) {
            String src = metaData.getColumnName(i + 1).toLowerCase();
            if (fieldsMap.containsKey(src)) {//仅处理已配置的字段
                destFields.add(fieldsMap.get(src));
                indexMap.put(src, indexMap.size());
            }
        }

        final String sql = String.format("insert into %s %s values %s", destTable,
                makeFieldsList(destFields), makeValuesPlaceholder(destFields.size()));

        logger.debug("SQL: " + sql);

        //准备执行插入
        PreparedStatement pstmt = destConn.prepareStatement(sql);

        int rows = 0;

        while (resultSet.next()) {

            //设置当前行数据
            for (String field : indexMap.keySet()) {

                Object value = resultSet.getObject(field);

                pstmt.setObject(indexMap.get(field) + 1, value);
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

    private String makeFieldsList(List<String> list) {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0, n = list.size(); i < n; ++i) {
            sb.append("`");
            sb.append(list.get(i));
            sb.append("`");
            if (i < n - 1) {
                sb.append(",");
            }
        }
        sb.append(")");
        return sb.toString();
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
                .replaceAll("<TABLE>", srcTable)
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
