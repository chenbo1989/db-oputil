package chenbo.cimiss.format;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by chenbo on 2019/6/20.
 */
public class Maps {
    private final static ValueExtractor def = new ValueExtractor() {
        @Override
        public Object extract(ResultSet resultSet, int col) throws SQLException {
            return resultSet.getObject(col);
        }
    };


    private final static ValueExtractor date = new ValueExtractor() {
        @Override
        public Object extract(ResultSet resultSet, int col) throws SQLException {
            Date date = resultSet.getDate(col);
            if(date==null) return "NULL";
            return date.toString();
        }
    };

    private final static ValueExtractor time = new ValueExtractor() {
        @Override
        public Object extract(ResultSet resultSet, int col) throws SQLException {
            Time time = resultSet.getTime(col);
            if(time==null) return "NULL";
            return time.toString();
        }
    };

    private final static ValueExtractor datetime = new ValueExtractor() {
        @Override
        public Object extract(ResultSet resultSet, int col) throws SQLException {
            Timestamp ts = resultSet.getTimestamp(col);
            if(ts==null) return "NULL";
            return ts.toString();
        }
    };

    private static Map<String, ValueExtractor> map = new HashMap<>();

    static {
        map.put("date", date);
        map.put("time", time);
        map.put("datetime", datetime);
        map.put("timestamp", datetime);
    }


    private Map<Integer, ValueExtractor> types = new HashMap<>();

    public Maps(ResultSetMetaData metaData) throws SQLException {
        int count = metaData.getColumnCount();
        for (int i = 1; i <= count; ++i) {
            String type = metaData.getColumnTypeName(i).toLowerCase();
            if (!map.containsKey(type)) {
                types.put(i, def);
            } else {
                types.put(i, map.get(type));
            }
        }
    }

    public ValueExtractor get(int col){
        return types.get(col);
    }
}
