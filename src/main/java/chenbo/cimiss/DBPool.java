package chenbo.cimiss;

import cn.golaxy.gkg.base.IniReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Created by chenbo on 2019/3/30.
 */
public class DBPool {
    private static Logger logger = LogManager.getLogger(DBPool.class);

    public static final String CONFIG_INI = "db-pool.cfg";

    private Map<String, Connection> connectionMap = new HashMap<>(4);

    private IniReader reader;

    private static final DBPool INSTANCE = new DBPool();

    public static DBPool getInstance() {
        return INSTANCE;
    }

    private DBPool(){
        this(CONFIG_INI);
    }

    private DBPool(String path) {
        reader = new IniReader(path);

        String prehot = reader.getGlobalValue("db.preInit");
        if (prehot != null) {
            for (String name : prehot.split("[,;\t]+")) {
                getConnection(name);
            }
        }
    }

    public Connection getConnection(String connectionName) {
        if (connectionMap.containsKey(connectionName)) {
            return connectionMap.get(connectionName);
        }
        Properties properties = reader.getSection(connectionName);
        if (properties == null) {
            logger.error("Config missing for " + connectionName);
            return null;
        }
        synchronized (this) {
            Connection conn = initConnection(properties);
            if (conn == null) {
                logger.error("Failed to get connection for " + connectionName);
            }
            connectionMap.put(connectionName, conn);
            return conn;
        }
    }

    private Connection initConnection(Properties properties) {
        String driverClass = properties.getProperty("driverClass");
        if(driverClass==null) {
            logger.error("driverClass is missing!");
            return null;
        }
        logger.debug(driverClass);

        try {
            Class.forName(driverClass);
        } catch (Exception ex) {
            logger.error(ex);
            return null;
        }

        String url = properties.getProperty("jdbcUrl");
        String username = properties.getProperty("user");
        String password = properties.getProperty("password");

        logger.debug(url);

        try {
            return DriverManager.getConnection(url, username, password);
        } catch (Exception ex) {
            logger.error(ex);
        }

        return null;
    }

    /**
     * 关闭所有连接并从当前缓存池中移除
     */
    public synchronized void closeAll() {
        Set<Map.Entry<String, Connection>> set = connectionMap.entrySet();
        for (Map.Entry<String, Connection> entry : set) {
            try {
                entry.getValue().close();
            } catch (Exception ex) {
                logger.error(ex);
            }

            set.remove(entry);
        }
    }
}
