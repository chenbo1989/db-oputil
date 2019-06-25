package chenbo.cimiss.mysqlbinlog2;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by chenbo on 2019/6/23.
 */
public class Utils {

    public static void printColumns(List<CanalEntry.Column> cols){
        String line = cols.stream()
                .map(column -> column.getName() + "=" + column.getValue())
                .collect(Collectors.joining(","));
        System.out.println(line);
    }

    public static Map<String, Object> asMap(List<CanalEntry.Column> cols) {
        Map<String, Object> map = new HashMap<>(128);

        cols.stream()
                .forEach(col -> {
                    map.put(col.getName(), col.getIsNull() ? null : col.getValue());
                });
        return map;
    }

    public static int setValues(Map<String, Object> record, PreparedStatement pstmt,
                                 List<String> cols, int colNum)
    throws SQLException {
        for (String col : cols) {
            pstmt.setObject(++colNum, record.get(col));
        }
        return colNum;
    }

    public static int setValues(List<CanalEntry.Column> row, PreparedStatement pstmt,
                                List<String> cols, int colNum)
            throws SQLException {

        Map<String, Object> record = asMap(row);

        for (String col : cols) {
            pstmt.setObject(++colNum, record.get(col));
        }
        return colNum;
    }

    public static String makeValuesPlaceholder(int n) {
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
}
