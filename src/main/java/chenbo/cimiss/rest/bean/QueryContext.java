package chenbo.cimiss.rest.bean;

import chenbo.cimiss.DataSet;

/**
 * Created by chenbo on 2019/6/19.
 */
public class QueryContext {
    private String query;
    private long took;
    private DataSet result;
    private long affected;

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public long getTook() {
        return took;
    }

    public void setTook(long took) {
        this.took = took;
    }

    public DataSet getResult() {
        return result;
    }

    public void setResult(DataSet result) {
        this.result = result;
    }

    public long getAffected() {
        return affected;
    }

    public void setAffected(long affected) {
        this.affected = affected;
    }
}
