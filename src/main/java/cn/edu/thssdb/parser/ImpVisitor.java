package cn.edu.thssdb.parser;


// TODO: add logic for some important cases, refer to given implementations and SQLBaseVisitor.java for structures

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.query.*;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.*;
import javafx.util.Pair;

import javax.management.AttributeNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * When use SQL sentence, e.g., "SELECT avg(A) FROM TableX;"
 * the parser will generate a grammar tree according to the rules defined in SQL.g4.
 * The corresponding terms, e.g., "select_stmt" is a root of the parser tree, given the rules
 * "select_stmt :
 *     K_SELECT ( K_DISTINCT | K_ALL )? result_column ( ',' result_column )*
 *         K_FROM table_query ( ',' table_query )* ( K_WHERE multiple_condition )? ;"
 *
 * This class "ImpVisit" is used to convert a tree rooted at e.g. "select_stmt"
 * into the collection of tuples inside the database.
 *
 * We give you a few examples to convert the tree, including create/drop/quit.
 * You need to finish the codes for parsing the other rooted trees marked TODO.
 */

public class ImpVisitor extends SQLBaseVisitor<Object> {
    private Manager manager;
    private long session;

    public ImpVisitor(Manager manager, long session) {
        super();
        this.manager = manager;
        this.session = session;
    }

    private Database GetCurrentDB() {
        Database currentDB = manager.getCurrentDatabase();
        if(currentDB == null) {
            throw new DatabaseNotExistException();
        }
        return currentDB;
    }

    public QueryResult visitSql_stmt(SQLParser.Sql_stmtContext ctx) {
        if (ctx.create_db_stmt() != null) return new QueryResult(visitCreate_db_stmt(ctx.create_db_stmt()));
        if (ctx.drop_db_stmt() != null) return new QueryResult(visitDrop_db_stmt(ctx.drop_db_stmt()));
        if (ctx.use_db_stmt() != null)  return new QueryResult(visitUse_db_stmt(ctx.use_db_stmt()));
        if (ctx.create_table_stmt() != null) return new QueryResult(visitCreate_table_stmt(ctx.create_table_stmt()));
        if (ctx.drop_table_stmt() != null) return new QueryResult(visitDrop_table_stmt(ctx.drop_table_stmt()));
        if (ctx.insert_stmt() != null) return new QueryResult(visitInsert_stmt(ctx.insert_stmt()));
        if (ctx.delete_stmt() != null) return new QueryResult(visitDelete_stmt(ctx.delete_stmt()));
        if (ctx.update_stmt() != null) return new QueryResult(visitUpdate_stmt(ctx.update_stmt()));
        if (ctx.select_stmt() != null) return visitSelect_stmt(ctx.select_stmt());
        if (ctx.quit_stmt() != null) return new QueryResult(visitQuit_stmt(ctx.quit_stmt()));
        return null;
    }

    /**
     创建数据库
     */
    @Override
    public String visitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx) {
        try {
            manager.createDatabaseIfNotExists(ctx.database_name().getText().toLowerCase());
            manager.persist();
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Create database " + ctx.database_name().getText() + ".";
    }

    /**
     删除数据库
     */
    @Override
    public String visitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx) {
        try {
            manager.deleteDatabase(ctx.database_name().getText().toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Drop database " + ctx.database_name().getText() + ".";
    }

    /**
     切换数据库
     */
    @Override
    public String visitUse_db_stmt(SQLParser.Use_db_stmtContext ctx) {
        try {
            manager.switchDatabase(ctx.database_name().getText().toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Switch to database " + ctx.database_name().getText() + ".";
    }

    /**
     删除表格
     */
    @Override
    public String visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
        try {
            GetCurrentDB().drop(ctx.table_name().getText().toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Drop table " + ctx.table_name().getText() + ".";
    }

    /**
     * TODO
     创建表格
     */
    @Override
    public String visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
        String table_name = ctx.table_name().getText();
        List<Column> columnList = new ArrayList<>();
        for (SQLParser.Column_defContext item : ctx.column_def()) {
            columnList.add(Column_defVisitor(item));
        }

        if (ctx.table_constraint() != null) {
            // 解析表的末端主键约束
            int size = ctx.table_constraint().column_name().size();
            String[] attrs = new String[size];
            for (int i = 0; i < size; i++) {
                attrs[i] = ctx.table_constraint().column_name(i).getText().toLowerCase();
            }
            for (String item : attrs) {
                boolean set = false;
                for (Column column : columnList) {
                    if (column.getColumnName().toLowerCase().equals(item)) {
                        column.setPrimary(1);
                        set = true;
                    }
                    if (!set) {
//                        throw new AttributeNotFoundException(item);
                        return "1111";
                    }
                }
            }
        }

        // 建表
        try {
            manager.getCurrentDatabase().create(table_name.toLowerCase(),
                    columnList.toArray(new Column[columnList.size()]));
        }
        catch (Exception e) {
            return e.getMessage();
        }
        return "Table " + table_name + " Created Successfully.";
    }

    // 读取列的定义
    public Column Column_defVisitor(SQLParser.Column_defContext ctx) {
        // 约束
        boolean flag_notnull = false;
        int flag_primary = 0;
        for (SQLParser.Column_constraintContext item : ctx.column_constraint()) {
            // 解析列约束
            ConstraintEnumsType constraint = null;
            if (item.K_PRIMARY() != null) {
                constraint = ConstraintEnumsType.PRIMARY;
            }
            else if (item.K_NULL() != null) {
                constraint = ConstraintEnumsType.NOTNULL;
            }
            if (constraint.equals(ConstraintEnumsType.PRIMARY)) {
                flag_primary = 1;
            }
            else if (constraint.equals(ConstraintEnumsType.NOTNULL)) {
                flag_notnull = true;
            }
            // 给主键添加not null约束
            flag_notnull = flag_notnull || (flag_primary == 1);
        }

        // 名称和类型
        String name = ctx.column_name().getText().toLowerCase();
        Pair<ColumnType, Integer> type = Type_nameVisitor(ctx.type_name());
        ColumnType columnType = type.getKey();
        int maxLength = type.getValue();
        return new Column(name, columnType, flag_primary, flag_notnull, maxLength);
    }

    // 读取列的类型和最大程度
    public Pair<ColumnType, Integer> Type_nameVisitor(SQLParser.Type_nameContext ctx) {
        if (ctx.T_INT() != null) { // INT
            return new Pair<>(ColumnType.INT, -1);
        }
        else if (ctx.T_LONG() != null) { // LONG
            return new Pair<>(ColumnType.LONG, -1);
        }
        else if (ctx.T_FLOAT() != null) { // FLOAT
            return new Pair<>(ColumnType.FLOAT, -1);
        }
        else if (ctx.T_DOUBLE() != null) { // DOUBLE
            return new Pair<>(ColumnType.DOUBLE, -1);
        }
        else if (ctx.T_STRING() != null) { // STRING
            try {
                return new Pair<>(ColumnType.STRING, Integer.parseInt(ctx.NUMERIC_LITERAL().getText()));
            } catch (Exception e) {
                throw new ParseStringColumnException(e.getMessage());
            }
        }
        else {
            return null;
        }
    }




    /**
     * TODO
     表格项插入
     */
    @Override
    public String visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {return null;}

    /**
     * TODO
     表格项删除
     */
    @Override
    public String visitDelete_stmt(SQLParser.Delete_stmtContext ctx) {return null;}

    /**
     * TODO
     表格项更新
     */
    @Override
    public String visitUpdate_stmt(SQLParser.Update_stmtContext ctx) {return null;}

    /**
     * TODO
     表格项查询
     */
    @Override
    public QueryResult visitSelect_stmt(SQLParser.Select_stmtContext ctx) {return null;}

    /**
     退出
     */
    @Override
    public String visitQuit_stmt(SQLParser.Quit_stmtContext ctx) {
        try {
            manager.quit();
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Quit.";
    }

    public Object visitParse(SQLParser.ParseContext ctx) {
        return visitSql_stmt_list(ctx.sql_stmt_list());
    }

    public Object visitSql_stmt_list(SQLParser.Sql_stmt_listContext ctx) {
        ArrayList<QueryResult> ret = new ArrayList<>();
        for (SQLParser.Sql_stmtContext subCtx : ctx.sql_stmt()) ret.add(visitSql_stmt(subCtx));
        return ret;
    }
}
