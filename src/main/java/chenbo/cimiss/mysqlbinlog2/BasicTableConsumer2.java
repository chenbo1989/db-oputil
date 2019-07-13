package chenbo.cimiss.mysqlbinlog2;

import chenbo.cimiss.TableMappingManager;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * 表格数据同步的消费者。配置文件相应配置为目标表。
 * 字段映射为：目标字段:源字段，可以支持不同目标字段 采用相同源字段
 *
 * Created by chenbo on 2019/6/23.
 */
public class BasicTableConsumer2 implements TableConsumer {
    private static Logger LOG = LogManager.getLogger(BasicTableConsumer2.class);

    private BatchCommit writer;

    private TableMappingManager mappingManager;


    public BasicTableConsumer2(String taskName, Properties properties, String srcDB, String destPool, String destDB) {
        writer = new BatchCommit(destPool);

        mappingManager = new TableMappingManager(taskName, properties, srcDB, destDB);

        LOG.info("SQL: " + mappingManager.getInsertSQLTemp());
        LOG.info("SQL: " + mappingManager.getUpdateSQLTemp());
        LOG.info("SQL: " + mappingManager.getDeleteSQLTemp());
    }


    @Override
    public boolean accept(String type, String schema, String table) {
        return schema.equals(mappingManager.getSrcDB()) && table.equals(mappingManager.getSrcTable());
    }

    @Override
    public void doInsert(List<CanalEntry.Column> cols) throws SQLException {
        final String sql = mappingManager.getInsertSQLTemp();
        PreparedStatement pstmt = writer.newStatement(sql);
        Utils.setValues(cols, pstmt, mappingManager.getSrcFields(), 0);
        writer.commit(sql, pstmt);
    }

    @Override
    public void doUpdate(List<CanalEntry.Column> before, List<CanalEntry.Column> after)throws SQLException {
        String sql = mappingManager.getUpdateSQLTemp();
        PreparedStatement pstmt = writer.newStatement(sql);
        //set
        int colNum = Utils.setValues(after, pstmt, mappingManager.getSrcFields(), 0);
        //where
        Utils.setValues(before, pstmt, mappingManager.getSrcPKCols(), colNum);
        writer.commit(sql, pstmt);
    }

    @Override
    public void doDelete(List<CanalEntry.Column> before) throws SQLException {
        final String sql = mappingManager.getDeleteSQLTemp();
        PreparedStatement pstmt = writer.newStatement(sql);
        Utils.setValues(before, pstmt, mappingManager.getSrcFields(), 0);
        writer.commit(sql, pstmt);
    }
}
