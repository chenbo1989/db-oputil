package chenbo.cimiss;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by chenbo on 2019/6/19.
 */
public class DBCache {
    private static final Map<String, JDBCBasicQuery> cache = new HashMap<>(16);

    public static JDBCBasicQuery getOrCreate(String database) {
        JDBCBasicQuery ret = cache.get(database);
        if (ret != null) {
            return ret;
        }
        synchronized (cache) {
            if (cache.containsKey(database)) {
                return cache.get(database);
            }
            ret = new JDBCBasicQuery(database);
            cache.put(database, ret);
            return ret;
        }
    }
}
