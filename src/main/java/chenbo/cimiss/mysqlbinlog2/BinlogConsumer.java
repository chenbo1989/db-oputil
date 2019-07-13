package chenbo.cimiss.mysqlbinlog2;

import cn.golaxy.gkg.base.IniReader;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Created by chenbo on 2019/6/20.
 */
public class BinlogConsumer {
    private static Logger logger = LogManager.getLogger(BinlogConsumer.class);

    private IniReader reader;

    private final String table_filter;

    private final String canal_host;
    private final int canal_port;
    private final String canal_dest;

    private String defaultSrcDB, defaultDestDB;
    private String defaultPool;

    private List<TableConsumer> consumers;

    private int fetchBatch;
    private int waitSeconds;

    private Pattern tablePattern;

    public BinlogConsumer(String cfg) {
        reader = new IniReader(cfg);

        this.canal_host = reader.getGlobalValue("canal.host", "127.0.0.1");
        this.canal_port = Integer.parseInt(reader.getGlobalValue("canal.port", "11111"));
        this.canal_dest = reader.getGlobalValue("canal.dest", "example");

        this.table_filter = reader.getGlobalValue("src.filter", ".*\\..*");

        String regex = reader.getGlobalValue("src.filter.local", ".*\\..*");
        tablePattern = Pattern.compile(regex);

        this.fetchBatch = Integer.parseInt(reader.getGlobalValue("fetch.batch", "100"));
        this.waitSeconds = Integer.parseInt(reader.getGlobalValue("wait", "1"));

        this.defaultSrcDB = reader.getGlobalValue("db.src");
        this.defaultDestDB = reader.getGlobalValue("db.dest");
        this.defaultPool = reader.getGlobalValue("db.pool");

        String taskList = reader.getGlobalValue("task.list");
        consumers = new ArrayList<>();
        for (String taskName : taskList.split("[,;]+")) {
            Properties properties = reader.getSection(taskName);
            TableConsumer consumer = new BasicTableConsumer2(taskName, properties, defaultSrcDB, defaultPool, defaultDestDB);
            consumers.add(consumer);

            logger.info("task[" + taskName + "] init");
        }
    }

    private void process(CanalEntry.EventType type, CanalEntry.RowData rowData, List<TableConsumer> cs) {
        for (TableConsumer consumer : cs) {
            try {
                if (type == CanalEntry.EventType.INSERT) {
                    consumer.doInsert(rowData.getAfterColumnsList());
                } else if (type == CanalEntry.EventType.UPDATE) {
                    consumer.doUpdate(rowData.getBeforeColumnsList(), rowData.getAfterColumnsList());
                } else if (type == CanalEntry.EventType.DELETE) {
                    consumer.doDelete(rowData.getBeforeColumnsList());
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private List<TableConsumer> findSuitableConsumers(CanalEntry.EventType type, String schema, String table) {
        List<TableConsumer> ret = new ArrayList<>();
        for (TableConsumer consumer : consumers) {
            if (consumer.accept(type.toString(), schema, table)) {
                ret.add(consumer);
            }
        }
        return ret;
    }


    private void process(List<CanalEntry.Entry> entryList) {
        for (CanalEntry.Entry entry : entryList) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
            final String schema = entry.getHeader().getSchemaName(),
                    table = entry.getHeader().getTableName();
            final String source = schema+"."+table;
            if(!tablePattern.matcher(source).matches()){
                logger.debug(source+" filterd!");
                continue;
            }

            try {
                CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                CanalEntry.EventType type = rowChange.getEventType();
                logger.info("[" + type + "]" + source);

                List<TableConsumer> sinks = findSuitableConsumers(type, schema, table);
                if(sinks.isEmpty()){
                    logger.warn("No suitable consumers.");
                    continue;
                }

                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    process(type, rowData, sinks);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public void start() {
        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(canal_host, canal_port), canal_dest, "", "");

        connector.connect();
        connector.subscribe(table_filter);

        while (true) {
            Message message = connector.getWithoutAck(fetchBatch);
            long batchId = message.getId();
            if (batchId == -1 || message.getEntries().isEmpty()) {
                try {
                    Thread.sleep(1000L * waitSeconds);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            } else {
                process(message.getEntries());
                connector.ack(batchId);
            }
        }
    }


    public static void main(String[] args) {
        String cfg = args.length > 0 ? args[0] : "binlog-consumers.cfg";
        new BinlogConsumer(cfg).start();
    }
}
