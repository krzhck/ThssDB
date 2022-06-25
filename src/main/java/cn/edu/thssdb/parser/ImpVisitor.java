package cn.edu.thssdb.parser;


// TODO: add logic for some important cases, refer to given implementations and SQLBaseVisitor.java for structures

import cn.edu.thssdb.common.Global;
import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.query.*;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.*;
import javafx.util.Pair;

import javax.management.AttributeNotFoundException;
import javax.management.Query;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.StringJoiner;

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
        if (ctx.show_meta_stmt() != null) return new QueryResult(visitShow_meta_stmt(ctx.show_meta_stmt()));
        if (ctx.show_table_stmt() != null) return new QueryResult(visitShow_table_stmt(ctx.show_table_stmt()));
        if (ctx.insert_stmt() != null) return new QueryResult(visitInsert_stmt(ctx.insert_stmt()));
        if (ctx.delete_stmt() != null) {
            return new QueryResult(visitDelete_stmt(ctx.delete_stmt()));
        }
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
                        return "Primary Key is not admissible!";
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

    @Override
    public String visitShow_table_stmt(SQLParser.Show_table_stmtContext ctx) {
        try {
            StringJoiner joiner = new StringJoiner("\n");
            ArrayList<String> tableNames = manager.get(ctx.database_name().getText()).getTableNameList();
            for (String tableName : tableNames)
                joiner.add(tableName);
            return joiner.toString();
        }
        catch (Exception e) {
            return e.getMessage();
        }
    }

    @Override
    public String visitShow_db_stmt(SQLParser.Show_db_stmtContext ctx) {return null;}

    /**
     表格项插入
     */
    @Override
    public String visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
        Database cur_database = GetCurrentDB();
        String table_name = ctx.table_name().getText().toLowerCase();
        ArrayList<String> column_names_list = new ArrayList<>();
        if (ctx.column_name() != null && ctx.column_name().size() != 0) {
            for (SQLParser.Column_nameContext item : ctx.column_name()) {
                column_names_list.add(item.getText().toLowerCase());
            }
        }
        String[] column_names = column_names_list.toArray(new String[column_names_list.size()]);

        if (manager.getSessionsInTransactions().contains(session)) {
            Table cur_table = cur_database.get(table_name);
            while (true) {
                if(!manager.getSessionsInLocks().contains(session)) {
                    int get_lock = cur_table.getXLock(session);
                    if(get_lock!=-1) {
                        if(get_lock==1) {
                            ArrayList<String> tmp = manager.x_lockDict.get(session);
                            tmp.add(table_name);
                            manager.x_lockDict.put(session,tmp);
                        }
                        break;
                    }
                    else {
                        manager.getSessionsInLocks().add(session);
                    }
                }
                else {
                    if(manager.getSessionsInLocks().get(0)==session)  //只查看阻塞队列开头session
                    {
                        int get_lock = cur_table.getXLock(session);
                        if(get_lock!=-1)
                        {
                            if(get_lock==1)
                            {
                                ArrayList<String> tmp = manager.x_lockDict.get(session);
                                tmp.add(table_name);
                                manager.x_lockDict.put(session,tmp);
                            }
                            manager.getSessionsInLocks().remove(0);
                            break;
                        }
                    }
                }
                try
                {
                    Thread.sleep(500);   // 休眠3秒
                } catch (Exception e) {
                    System.out.println("Got an exception!");
                }
            }
            for (SQLParser.Value_entryContext subcontext : ctx.value_entry())
            {
                String[] values = visitValue_entry(subcontext);
                try {
//                    if(column_names == null || column_names.length == 0)
//                    {
//                        Row r = new Row(values);
//                        cur_table.insert_single_row(values);
//                    }
//                    else
//                    {
                        cur_table.insert_single_row(column_names, values);
//                    }
                } catch (Exception e) {
                    return e.toString();
                }
            }
        }
        else {
            for (SQLParser.Value_entryContext item : ctx.value_entry()) {
                String[] values = visitValue_entry(item);
                try {
                    cur_database.insert(table_name, column_names, values);
                } catch (Exception e) {
                    return e.toString();
                }
            }
        }
        return "Inserted " + ctx.value_entry().size() + " rows.";
    }

    @Override
    public String visitShow_meta_stmt(SQLParser.Show_meta_stmtContext ctx) {
        try {
            return GetCurrentDB().getTableInfo(ctx.table_name().getText());
        }
        catch (Exception e) {
            return e.getMessage();
        }
    }

    public String[] visitValue_entry(SQLParser.Value_entryContext context) {
        String[] values = new String[context.literal_value().size()];
        for (int i = 0; i < context.literal_value().size(); i++) {
            values[i] = context.literal_value(i).getText();
        }
        return values;
    }

    /**
     表格项删除
     */
    @Override
    public String visitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
        Database the_database=GetCurrentDB();
        String table_name = ctx.table_name().getText().toLowerCase();
        if (ctx.K_WHERE() == null) {
            try {
                return the_database.delete(table_name, null);
            } catch (Exception e) {
                return e.toString();
            }
        }
        Logic logic = Multiple_conditionVisitor(ctx.multiple_condition());

        if(manager.getSessionsInTransactions().contains(session))
        {
            //manager.getSessionsInLocks().add(session);
            Table the_table = the_database.get(table_name);
            while(true)
            {
                if(!manager.getSessionsInLocks().contains(session))   //新加入一个session
                {
                    int get_lock = the_table.getXLock(session);
                    if(get_lock!=-1)
                    {
                        if(get_lock==1)
                        {
                            ArrayList<String> tmp = manager.x_lockDict.get(session);
                            tmp.add(table_name);
                            manager.x_lockDict.put(session,tmp);
                        }
                        break;
                    }else
                    {
                        manager.getSessionsInLocks().add(session);
                    }
                }else    //之前等待的session
                {
                    if(manager.getSessionsInLocks().get(0)==session)  //只查看阻塞队列开头session
                    {
                        int get_lock = the_table.getXLock(session);
                        if(get_lock!=-1)
                        {
                            if(get_lock==1)
                            {
                                ArrayList<String> tmp = manager.x_lockDict.get(session);
                                tmp.add(table_name);
                                manager.x_lockDict.put(session,tmp);
                            }
                            manager.getSessionsInLocks().remove(0);
                            break;
                        }
                    }
                }
                try
                {
                    Thread.sleep(500);
                } catch (Exception e) {
                    System.out.println("Got an exception!");
                }
            }

            try {
                return the_table.delete(logic);
            } catch (Exception e) {
                return e.toString();
            }

        }
        else{
            try {
                return the_database.delete(table_name, logic);
            } catch (Exception e) {
                return e.toString();
            }
        }
    }

    public Logic Multiple_conditionVisitor(SQLParser.Multiple_conditionContext context) {
        if (context.condition() != null)
            return visitCondition(context.condition());
        LogicAtom logicType;
        if (context.AND() != null)
            logicType = LogicAtom.and;
        else
            logicType = LogicAtom.or;
        return new Logic(Multiple_conditionVisitor(context.multiple_condition(0)),
                logicType,
                Multiple_conditionVisitor(context.multiple_condition(1)));
    }

    public Logic visitCondition(SQLParser.ConditionContext context) {
        Comparer left = ExpressionVisitor(context.expression(0));
        Comparer right = ExpressionVisitor(context.expression(1));
        LogicAtom type = ComparatorVisitor(context.comparator());
        return new Logic(left, type, right);
    }

    public Comparer ExpressionVisitor(SQLParser.ExpressionContext context) {
        if (context.comparer() != null) {
            return ComparerVisitor(context.comparer());
        }
        else {
            return null;
        }
    }

    public LogicAtom ComparatorVisitor(SQLParser.ComparatorContext context) {
        if (context.EQ() != null) {
            return LogicAtom.eq;
        }
        else if (context.NE() != null) {
            return LogicAtom.neq;
        }
        else if (context.GT() != null) {
            return LogicAtom.great;
        }
        else if (context.LT() != null) {
            return LogicAtom.less;
        }
        else if (context.GE() != null) {
            return LogicAtom.geq;
        }
        else if (context.LE() != null) {
            return LogicAtom.leq;
        }
        return null;
    }

    public Comparer ComparerVisitor(SQLParser.ComparerContext context) {
        //处理column情况
        if (context.column_full_name() != null) {
            return new Comparer(ComparerType.COLUMN, context.column_full_name().getText());
        }
        //获得类型和内容
        LogicAtom type = Literal_valueVisitor(context.literal_value());
        String text = context.literal_value().getText();
        switch (type) {
            case num:
                return new Comparer(ComparerType.NUMBER, text);
            case str:
                return new Comparer(ComparerType.STRING, text.substring(1, text.length() - 1));
            case nul:
                return new Comparer(ComparerType.NULL, null);
            default:
                return null;
        }
    }

    public LogicAtom Literal_valueVisitor(SQLParser.Literal_valueContext context) {
        if (context.NUMERIC_LITERAL() != null) {
            return LogicAtom.num;
        }
        if (context.STRING_LITERAL() != null) {
            return LogicAtom.str;
        }
        if (context.K_NULL() != null) {
            return LogicAtom.nul;
        }
        return null;
    }

    /**
     表格项更新
     */
    @Override
    public String visitUpdate_stmt(SQLParser.Update_stmtContext ctx) {
        Database database = GetCurrentDB();
        String table_name = ctx.table_name().getText().toLowerCase();
        String column_name = ctx.column_name().getText().toLowerCase();
        Comparable value = ExpressionVisitor(ctx.expression()).mValue;
        String s_value = (value != null ? value.toString() : Global.ENTRY_NULL);
        SQLParser.Multiple_conditionContext multiple_condition = ctx.multiple_condition();
        if(multiple_condition == null){
            try {
                return database.update_rows(table_name, column_name, s_value, null);
            } catch (Exception e) {
                return e.toString();
            }
        }

        Logic logic = Multiple_conditionVisitor(multiple_condition);
        if (manager.getSessionsInTransactions().contains(session)) {
            Table table = database.get(table_name);
            while (true) {
                if (!manager.getSessionsInLocks().contains(session)) { // 新加入一个session
                    int get_lock = table.getXLock(session);
                    if (get_lock != -1) {
                        if(get_lock==1)
                        {
                            ArrayList<String> tmp = manager.x_lockDict.get(session);
                            tmp.add(table_name);
                            manager.x_lockDict.put(session,tmp);
                        }
                        break;
                    }
                    else {
                        manager.getSessionsInLocks().add(session);
                    }
                }
                else {
                    if(manager.getSessionsInLocks().get(0)==session)  //只查看阻塞队列开头session
                    {
                        int get_lock = table.getXLock(session);
                        if(get_lock!=-1)
                        {
                            if(get_lock==1)
                            {
                                ArrayList<String> tmp = manager.x_lockDict.get(session);
                                tmp.add(table_name);
                                manager.x_lockDict.put(session,tmp);
                            }
                            manager.getSessionsInLocks().remove(0);
                            break;
                        }
                    }
                }
                try
                {
                    Thread.sleep(500);
                } catch (Exception e) {
                    System.out.println("Got an exception!");
                }
            }

            try {
                return database.update_rows(table_name, column_name, s_value, logic);
            } catch (Exception e) {
                return e.toString();
            }
        }
        else{
            try {
                return database.update_rows(table_name, column_name, s_value, logic);
            } catch (Exception e) {
                return e.toString();
            }
        }
    }

    /**
     * TODO
     表格项查询
     */
    @Override
    public QueryResult visitSelect_stmt(SQLParser.Select_stmtContext ctx) {
        Database cur_database = GetCurrentDB();
        boolean distinct = false;
        if (ctx.K_DISTINCT() != null) {
            distinct = true;
        }
        // 获取列名
        int col_count = ctx.result_column().size();
        String[] col_selected = new String[col_count];
        for (int i = 0; i < col_count; i++) {
            String col_name = ctx.result_column(i).getText().toLowerCase();
            if (col_name.equals("*")) {
                col_selected = null;
                break;
            }
            col_selected[i] = col_name;
        }

        // 建立querytable
        int query_count = ctx.table_query().size();
        if (query_count == 0) {
            throw new WithoutFromTableException();
        }
        QueryTable cur_query_table = null;
        ArrayList<String> table_names = new ArrayList<>();

        try {
            cur_query_table = Table_queryVisitor(ctx.table_query(0));
            for (SQLParser.Table_nameContext subctx : ctx.table_query(0).table_name()) {
                table_names.add(subctx.getText().toLowerCase());
            }
        } catch (Exception e) {
            return new QueryResult(e.toString());
        }
        if (cur_query_table == null) {
            throw new WithoutFromTableException();
        }
        // 建立逻辑，获得结果
        Logic logic = null;
        if (ctx.K_WHERE() != null) {
            logic = Multiple_conditionVisitor(ctx.multiple_condition());
        }
        cur_query_table.setLogic(logic);

        if (manager.getSessionsInTransactions().contains(session)) {
            while(true) {
                if (!manager.getSessionsInLocks().contains(session)) {
                    ArrayList<Integer> lock_res = new ArrayList<>();
                    for (String name : table_names) {
                        Table cur_table = cur_database.get(name);
                        int get_lock = cur_table.getSLock(session);
                        lock_res.add(get_lock);
                    }
                    if (lock_res.contains(-1)) {
                        for (String name : table_names) {
                            Table cur_table = cur_database.get(name);
                            cur_table.releaseSLock(session);
                        }
                        manager.getSessionsInLocks().add(session);
                    }
                    else {
                        break;
                    }
                }
                else {
                    if(manager.getSessionsInLocks().get(0)==session)  //只查看阻塞队列开头session
                    {
                        ArrayList<Integer> lock_res = new ArrayList<>();
                        for (String name : table_names) {
                            Table cur_table = cur_database.get(name);
                            int get_lock = cur_table.getSLock(session);
                            lock_res.add(get_lock);
                        }
                        if(!lock_res.contains(-1))
                        {
                            manager.getSessionsInLocks().remove(0);
                            break;
                        }else
                        {
                            for (String name : table_names) {
                                Table cur_table = cur_database.get(name);
                                cur_table.releaseSLock(session);
                            }
                            String msg = "Exception: There exists uncommitted changes!";
                            return new QueryResult(msg);
                        }
                    }
                }
                try {
                    Thread.sleep(500);
                }
                catch (Exception e) {
                    System.out.println("Got an Exception!");
                }
            }
            try {
                for (String name : table_names) {
                    Table cur_table = cur_database.get(name);
                    cur_table.releaseSLock(session);
                }
                return cur_database.select(cur_query_table, col_selected, distinct);
            } catch (Exception e) {
                return new QueryResult(e.toString());
            }
        }
        else {
            try {
                return cur_database.select(cur_query_table, col_selected, distinct);
            } catch (Exception e) {
                return new QueryResult(e.toString());
            }
        }
    }

    public QueryTable Table_queryVisitor(SQLParser.Table_queryContext ctx) {
        Database cur_database = GetCurrentDB();
        if (ctx.K_JOIN().size() == 0) { // 单一表
            return cur_database.getSingleQueryTable(ctx.table_name(0).getText().toLowerCase());
        }
        else { // 复合表
            Logic logic = Multiple_conditionVisitor(ctx.multiple_condition());
            ArrayList<String> table_names = new ArrayList<>();
            for (SQLParser.Table_nameContext subctx : ctx.table_name()) {
                table_names.add(subctx.getText().toLowerCase());
            }
            return cur_database.getMultiQueryTable(table_names, logic);
        }
    }

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
