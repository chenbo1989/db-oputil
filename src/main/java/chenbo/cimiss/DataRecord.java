package chenbo.cimiss;

import java.util.*;
/**
 * Created by chenbo on 2019/6/19.
 */
public class DataRecord {
    private List<Object> values;

    public DataRecord(List<Object> values){
        this.values = values;
    }

    public List<Object> getValues() {
        return values;
    }

    public void setValues(List<Object> values) {
        this.values = values;
    }
}
