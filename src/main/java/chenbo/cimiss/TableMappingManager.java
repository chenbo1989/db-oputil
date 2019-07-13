package chenbo.cimiss;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * 工具类，管理表映射关系。配置如下：
 * src_field[:dest_field][,src_field[:dest_field]*
 * Created by chenbo on 2019/7/13.
 */
public class TableMappingManager {
    private static Logger LOG = LogManager.getLogger(TableMappingManager.class);

    private String srcDB, srcTable;
    private String destDB, destTable;

    //目标表字段->源表字段
    private Map<String, String> fieldsMap = new HashMap<>();

    private List<String> destFields = new ArrayList<>(64);

    //目标字段对应的源字段列表 可能重复
    private List<String> srcFields = new ArrayList<>(64);

    //的PK字段
    private List<String> srcPKCols = new ArrayList<>(4);

    private String insertSQLTemp, deleteSQLTemp, updateSQLTemp;

    public TableMappingManager(String defaultTableName, Properties properties, String defaultSrcDB, String defaultDestDB){
        this.srcDB = properties.getProperty("db.src", defaultSrcDB);
        this.destDB = properties.getProperty("db.dest", defaultDestDB);

        String table = properties.getProperty("table", defaultTableName);
        this.srcTable = properties.getProperty("table.src", table);
        this.destTable = properties.getProperty("table.dest", this.srcTable);

        LOG.info(String.format("tableMapping [%s.%s] -> [%s.%s]", srcDB, srcTable, destDB, destTable));

        this.parseMapping(properties);
    }


    public void parseMapping(Properties properties){
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

            fieldsMap.put(to, from);

            destFields.add(to);
            srcFields.add(from);

            destSet.append(to + "=?");
            insertCols.append("`"+to+"`");
            insertValues.append("?");
            if (i < aMappings.length - 1) {
                destSet.append(",");
                insertCols.append(",");
                insertValues.append(",");
            }
        }

        this.insertSQLTemp = String.format("insert into `%s`.`%s`(%s) values(%s)", destDB, destTable, insertCols.toString(), insertValues.toString());

        //配置中获取目标PK字段
        String pks = properties.getProperty("pk");
        if(pks==null){
            LOG.warn("Empty pk configured");
            return;
        }

        List<String> destPKCols = new ArrayList<>(4);
        for(String pk: pks.split("[,;\t]+")){
            destPKCols.add(pk.trim());
        }

        //构造目标的where条件
        StringBuilder whereCond = new StringBuilder();
        final int nColsMinusOne = destPKCols.size() - 1;
        for (int i = 0; i <=nColsMinusOne; ++i) {
            String toPK = destPKCols.get(i);
            whereCond.append( toPK + "=?");
            if (i < nColsMinusOne) {
                whereCond.append(" and ");
            }

            this.srcPKCols.add(fieldsMap.get(toPK));
        }

        final String where = whereCond.toString();
        if(where.isEmpty()){
            LOG.warn("Empty PK configured! UPDATE/DELETE can't work");
        }

        this.deleteSQLTemp = String.format("delete from `%s`.`%s` where %s", destDB, destTable, where);
        this.updateSQLTemp = String.format("update `%s`.`%s` set %s where %s", destDB, destTable, destSet.toString(), where);
    }


    public String getSrcDB() {
        return srcDB;
    }

    public String getSrcTable() {
        return srcTable;
    }

    public String getDestDB() {
        return destDB;
    }


    public String getDestTable() {
        return destTable;
    }

    public Map<String, String> getFieldsMap() {
        return fieldsMap;
    }

    public List<String> getSrcFields() {
        return srcFields;
    }


    public List<String> getSrcPKCols() {
        return srcPKCols;
    }

    public String getInsertSQLTemp() {
        return insertSQLTemp;
    }


    public String getDeleteSQLTemp() {
        return deleteSQLTemp;
    }

    public String getUpdateSQLTemp() {
        return updateSQLTemp;
    }

    public List<String> getDestFields() {
        return destFields;
    }
}
