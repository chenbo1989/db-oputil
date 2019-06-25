package chenbo.cimiss.rest;

import chenbo.cimiss.CountQuery;
import chenbo.cimiss.DBCache;
import chenbo.cimiss.DataSet;
import chenbo.cimiss.JDBCBasicQuery;
import chenbo.cimiss.rest.bean.QueryContext;
import com.sun.jersey.spi.resource.Singleton;
import ict.ada.common.rest.bean.WebResult;
import ict.ada.common.rest.util.ResUtil;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

/**
 * 统计分析服务
 *
 * Created by chenbo on 2019/4/16.
 */
@Path("/db")
@Singleton
public class DataResource {

    @GET
    @Path("/{database}/_query")
    public Response query(
            @PathParam("database") String database,
            @QueryParam("sql") String sql
    ) {
        if (database == null || database.isEmpty() || sql == null || sql.isEmpty()) {
            return ResUtil.build(WebResult.error(1, WebResult.ERROR_PARAM));
        }
        JDBCBasicQuery query = DBCache.getOrCreate(database);
        try {
            long t1 = System.currentTimeMillis();
            DataSet ds = query.query(sql);
            long t2 = System.currentTimeMillis();
            QueryContext context = new QueryContext();
            context.setQuery(sql);
            context.setTook(t2 - t1);
            context.setResult(ds);
            return ResUtil.build(new WebResult(context));
        } catch (Exception ex) {
            return ResUtil.build(WebResult.error(10, ex.getMessage()));
        }
    }

    @GET
    @Path("/{database}/_execute")
    public Response execute(
            @PathParam("database") String database,
            @QueryParam("sql") String sql
    ) {
        if (database == null || database.isEmpty() || sql == null || sql.isEmpty()) {
            return ResUtil.build(WebResult.error(1, WebResult.ERROR_PARAM));
        }
        JDBCBasicQuery query = DBCache.getOrCreate(database);
        try {
            long t1 = System.currentTimeMillis();
            long affected = query.execute(sql);
            long t2 = System.currentTimeMillis();
            QueryContext context = new QueryContext();
            context.setQuery(sql);
            context.setTook(t2 - t1);
            context.setAffected(affected);
            return ResUtil.build(new WebResult(context));
        } catch (Exception ex) {
            return ResUtil.build(WebResult.error(10, ex.getMessage()));
        }
    }


    /**
     * 统计数据库行数
     * @param database
     * @param tables
     * @return
     */
    @GET
    @Path("/{database}/{tables}/_count")
    public Response countTable(
            @PathParam("database") String database,
            @PathParam("tables") String tables
    ) {
        if(database==null || database.isEmpty() || tables==null || tables.isEmpty()){
            return ResUtil.build(WebResult.error(1, WebResult.ERROR_PARAM));
        }
        Map<String, Long> counter = new HashMap<>();
        CountQuery query = new CountQuery(database);
        try {
            for (String table : tables.split(",")) {
                long count = query.count(table);
                counter.put(table, count);
            }
            return ResUtil.build(new WebResult(counter));
        } catch (Exception ex) {
            return ResUtil.build(WebResult.error(10, ex.getMessage()));
        }
    }
}
