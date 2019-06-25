package chenbo.cimiss;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class JDBCBasicQuery {
	private static Logger logger = LogManager.getLogger(JDBCBasicQuery.class.getName());

	protected Connection conn;
	protected int defaultTimeOut = 120;
	protected int maxRows = 500;

	public JDBCBasicQuery(String database) {
		conn = DBPool.getInstance().getConnection(database);
		if (conn == null) {
			logger.error("cannot find a proper connection:" + database);
			throw new RuntimeException(
					"Error:Cannot find a proper connection from " + database);
		}
	}

	public List<ColumnDef> getColumns(ResultSetMetaData metaData)throws Exception {
		final int columnCount = metaData.getColumnCount();
		List<ColumnDef> list = new ArrayList<>(columnCount);
		for(int i=1; i<=columnCount; ++i){
			String name = metaData.getColumnName(i), type = metaData.getColumnTypeName(i);
			ColumnDef column = new ColumnDef(name, type);

			list.add(column);
		}

		return list;
	}

	public List<DataRecord> getRows(ResultSet resultSet, final int columnCount) throws Exception {
		List<DataRecord> records = new ArrayList<>(16);
		ResultSetMetaData metaData = resultSet.getMetaData();
		while (records.size() < maxRows && resultSet.next()) {
			List<Object> values = new ArrayList<>(columnCount);
			for (int col = 1; col <= columnCount; ++col) {
				final String type = metaData.getColumnTypeName(col).toLowerCase();
				if (type.equals("date")) {
					values.add(DateUtils.formatDate(resultSet.getDate(col)));
				} else if (type.equals("datetime") || type.equals("timestamp")) {
					values.add(DateUtils.formatDatetime(resultSet.getTimestamp(col)));
				} else if (type.equals("time")) {
					values.add(DateUtils.formatTime(resultSet.getTime(col)));
				} else if ("binary".equals(type) || "blob".equals(type) || "clob".equals(type)) {
					values.add("[binary object]");
				} else {
					values.add(resultSet.getObject(col));
				}
			}

			records.add(new DataRecord(values));
		}
		return records;
	}

	public DataSet query(String sql)throws Exception {
		Statement stmt = conn.createStatement();
		stmt.setQueryTimeout(defaultTimeOut);
		//stmt.setMaxRows(maxRows);
		ResultSet rst = stmt.executeQuery(sql);
		//rst.setFetchSize(maxRows);
		DataSet dataSet = new DataSet();
		dataSet.setHeader(getColumns(rst.getMetaData()));
		dataSet.setRows(getRows(rst, dataSet.getHeader().size()));
		rst.close();
		stmt.close();
		return dataSet;
	}

	public long execute(String sql) throws Exception {
		Statement stmt = conn.createStatement();
		stmt.setQueryTimeout(defaultTimeOut);
		long ret = stmt.executeLargeUpdate(sql);
		stmt.close();
		return ret;
	}

	public Connection getConn() {
		return conn;
	}
}
