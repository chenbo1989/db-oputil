package chenbo.cimiss.mysqlbinlog2;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.List;

/**
 * Created by chenbo on 2019/6/23.
 */
public class JsonMessageConsumer implements TableConsumer {
    private Gson gson = new GsonBuilder().create();



    @Override
    public boolean accept(String type, String schema, String table) {
        return true;
    }

    @Override
    public void doInsert(List<CanalEntry.Column> cols) {

    }
}
