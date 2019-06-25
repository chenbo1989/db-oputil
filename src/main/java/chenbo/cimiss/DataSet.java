package chenbo.cimiss;

import java.util.*;

/**
 * Created by chenbo on 2019/6/19.
 */
public class DataSet {
    private List<ColumnDef> header;

    private List<DataRecord> rows;

    public List<ColumnDef> getHeader() {
        return header;
    }

    public void setHeader(List<ColumnDef> header) {
        this.header = header;
    }

    public List<DataRecord> getRows() {
        return rows;
    }

    public void setRows(List<DataRecord> rows) {
        this.rows = rows;
    }
}
