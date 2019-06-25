package chenbo.cimiss;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by chenbo on 2019/4/1.
 */
public class TableMapQuery extends JDBCBasicQuery {
    private Map<String, List<ColumnDef>> columns = new HashMap<>(16);

    public TableMapQuery(String database){
        super(database);
    }

    public List<ColumnDef> getTableColumns(String database, String table) throws SQLException {
        String sql = "show columns from " + database+"."+ table;

        PreparedStatement stmt = conn.prepareStatement(sql);

        ResultSet rs = stmt.executeQuery();

        List<ColumnDef> list = new ArrayList<>();

        while (rs.next()) {
            String name = rs.getString(1);
            String type = rs.getString(2);

            ColumnDef column = new ColumnDef(name, type);
            column.setNullable("YES".equals(rs.getString(3)));
            column.setKey(rs.getString(4));

            list.add(column);
        }

        rs.close();
        stmt.close();

        return list;
    }

    private List<ColumnDef> getDefMap(String database, String table) throws SQLException {
        final String key = String.format("%s.%s", database, table);
        if (columns.containsKey(key)) {
            return columns.get(key);
        }

        List<ColumnDef> list = getTableColumns(database, table);
        columns.put(key, list);
        return list;
    }
}
