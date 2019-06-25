package chenbo.cimiss.mysqlbinlog2;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;

/**
 * Created by chenbo on 2019/6/23.
 */
public interface TableConsumer {
    boolean accept(String type, String schema, String table);

    default void doInsert(List<CanalEntry.Column> cols) throws Exception {
        Utils.printColumns(cols);
    }

    default void doUpdate(List<CanalEntry.Column> before, List<CanalEntry.Column> after) throws Exception {
        Utils.printColumns(before);
        System.out.println("--> ");
        Utils.printColumns(after);
    }

    default void doDelete(List<CanalEntry.Column> before) throws Exception {
        Utils.printColumns(before);
    }
}
