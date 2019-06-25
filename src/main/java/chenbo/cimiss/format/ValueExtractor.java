package chenbo.cimiss.format;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by chenbo on 2019/6/20.
 */
public interface ValueExtractor {
    Object extract(ResultSet resultSet, int col) throws SQLException;
}
