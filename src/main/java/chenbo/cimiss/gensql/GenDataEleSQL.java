package chenbo.cimiss.gensql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.util.JdbcConstants;

import java.util.List;
import java.util.Scanner;

/**
 * Created by chenbo on 2019/6/2.
 */
public class GenDataEleSQL {

    public static String getInputSQL() {
        StringBuilder sb = new StringBuilder();
        Scanner sc = new Scanner(System.in);
        String line;
        while ((line = sc.nextLine()) != null) {
            sb.append(" " + line);
            if (line.equals(";")) break;
        }

        return sb.toString();
    }

    public static String getString1(){
        return "create table test1 (a varchar(20) not null primary key comment 'This is A')";
    }

    private static final String SQL_FORMAT = "INSERT INTO API_DATA_ELE_DEFINE values('%s', '%s', '%s', '%s', 'N', '-', '-', 'N', 'N');\n";

    public static void makeDataEleSQL(String allComment){
        final String DB_TYPE = JdbcConstants.MYSQL;
        final String sql = getInputSQL();
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, DB_TYPE);

        for(SQLStatement stmt: stmtList) {

            stmt.accept(new MySqlASTVisitorAdapter() {

                public boolean visit(SQLColumnDefinition x) {
                    String columnName = x.getNameAsString().trim();
                    if (columnName.startsWith("`")) {
                        columnName = columnName.substring(1);
                    }
                    if (columnName.endsWith("`")) {
                        columnName = columnName.substring(0, columnName.length() - 1);
                    }

                    columnName = columnName.toUpperCase();

                    String comment = "-";
                    if (x.getComment() != null) {
                        comment = x.getComment().toString();
                    }

                    System.out.printf(SQL_FORMAT,
                            columnName,
                            columnName,
                            comment,
                            allComment);

                    return true;
                }
            });

        }

    }

    public static void main(String[] args) {
        makeDataEleSQL(args.length>0?args[0]:"local-data-table");
    }
}
