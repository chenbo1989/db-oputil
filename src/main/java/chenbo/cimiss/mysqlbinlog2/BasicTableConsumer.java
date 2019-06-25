package chenbo.cimiss.mysqlbinlog2;

import com.alibaba.otter.canal.protocol.CanalEntry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by chenbo on 2019/6/23.
 */
public class BasicTableConsumer implements TableConsumer {
    private static Logger LOG = LogManager.getLogger(BasicTableConsumer.class);

    private String srcDB, srcTable;
    private String destDB, destTable;

    private BatchCommit writer;

    private Map<String, String> fieldsMap = new HashMap<>();

    private List<String> srcPKCols = new ArrayList<>(4);
    private List<String> srcFields = new ArrayList<>(64);

    private String insertSQLTemp, deleteSQLTemp, updateSQLTemp;

    public BasicTableConsumer(String taskName, Properties properties, String srcDB, String destPool, String destDB) {
        writer = new BatchCommit(destPool);

        this.srcDB = properties.getProperty("db.src", srcDB);
        this.destDB = properties.getProperty("db.dest", destDB);

        String table = properties.getProperty("table", taskName);
        this.srcTable = properties.getProperty("table.src", table);
        this.destTable = properties.getProperty("table.dest", this.srcTable);

        String pks = properties.getProperty("pk");
        if (pks != null) {
            for(String pk: pks.split("[,;\t]+")){
                this.srcPKCols.add(pk.trim());
            }
        }
        makeSQLWhere(properties);

        LOG.info(String.format("binlog [%s.%s] -> [%s.%s]", srcDB, srcTable, destDB, destTable));
        LOG.info("SQL: " + insertSQLTemp);
        LOG.info("SQL: " + updateSQLTemp);
        LOG.info("SQL: " + deleteSQLTemp);
    }

    private void makeSQLWhere(Properties properties) {
        //String pks =  properties.getProperty("pk.dest", srcPks);
        //pks.split("[,;\t]+")
        String mapping = properties.getProperty("fields.mapping");
        String[] aMappings = mapping.split("[,;\t]+");
        StringBuilder destSet = new StringBuilder();
        StringBuilder insertCols = new StringBuilder();
        StringBuilder insertValues = new StringBuilder();
        for (int i = 0; i < aMappings.length; ++i) {
            String[] a2b = aMappings[i].split(":");
            String from = a2b[0].trim().toLowerCase();
            String to = from;
            if (a2b.length == 2) {
                to = a2b[1].trim().toLowerCase();
            }

            fieldsMap.put(from, to);

            srcFields.add(from);

            destSet.append(to + "=?");
            insertCols.append(to);
            insertValues.append("?");
            if (i < aMappings.length - 1) {
                destSet.append(",");
                insertCols.append(",");
                insertValues.append(",");
            }
        }

        final String set = destSet.toString();

        final int nColsMinusOne = this.srcPKCols.size() - 1;
        StringBuilder destPKCond = new StringBuilder();
        for (int i = 0; i <=nColsMinusOne; ++i) {
            destPKCond.append(fieldsMap.get(this.srcPKCols.get(i)) + "=?");//映射到目标表的列名
            if (i < nColsMinusOne) {
                destPKCond.append(" and ");
            }
        }
        final String where = destPKCond.toString();

        this.insertSQLTemp = String.format("insert into `%s`.`%s`(%s) values(%s)", destDB, destTable, insertCols.toString(), insertValues.toString());
        this.deleteSQLTemp = String.format("delete from `%s`.`%s` where %s", destDB, destTable, where);
        this.updateSQLTemp = String.format("update `%s`.`%s` set %s where %s", destDB, destTable, set, where);
    }

    @Override
    public boolean accept(String type, String schema, String table) {
        return schema.equals(srcDB) && table.equals(srcTable);
    }

    @Override
    public void doInsert(List<CanalEntry.Column> cols) throws SQLException {
        PreparedStatement pstmt = writer.newStatement(insertSQLTemp);
        Utils.setValues(cols, pstmt, srcFields, 0);
        writer.commit(insertSQLTemp, pstmt);
    }

    @Override
    public void doUpdate(List<CanalEntry.Column> before, List<CanalEntry.Column> after)throws SQLException {
        PreparedStatement pstmt = writer.newStatement(updateSQLTemp);
        //set
        int colNum = Utils.setValues(after, pstmt, srcFields, 0);
        //where
        Utils.setValues(before, pstmt, srcPKCols, colNum);
        writer.commit(updateSQLTemp, pstmt);
    }

    @Override
    public void doDelete(List<CanalEntry.Column> before) throws SQLException {
        PreparedStatement pstmt = writer.newStatement(deleteSQLTemp);
        Utils.setValues(before, pstmt, srcPKCols, 0);
        writer.commit(deleteSQLTemp, pstmt);
    }
}
