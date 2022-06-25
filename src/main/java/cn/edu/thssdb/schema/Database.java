package cn.edu.thssdb.schema;


import cn.edu.thssdb.exception.DuplicateTableException;
import cn.edu.thssdb.exception.FileIOException;
import cn.edu.thssdb.exception.PageNotExistException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.query.Logic;

import cn.edu.thssdb.exception.*;

import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.common.Global;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

  private String databaseName;
  private HashMap<String, Table> tableMap;
  ReentrantReadWriteLock lock;

  public Database(String databaseName) {
    this.databaseName = databaseName;
    this.tableMap = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    File tableFolder = new File(this.getDatabaseTableFolderPath());
    if(!tableFolder.exists())
      tableFolder.mkdirs();
    recover();
  }


  // Operations: (basic) persist, create tables
  private void persist() {
    for (Table table : this.tableMap.values()) {
      String filename = table.getTableMetaPath();
      ArrayList<Column> columns = table.columns;
      try {
        FileOutputStream fileOutputStream = new FileOutputStream(filename);
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream);
        for (Column column : columns)
          outputStreamWriter.write(column.toString() + "\n");
        outputStreamWriter.close();
        fileOutputStream.close();
      } catch (Exception e) {
        throw new FileIOException(filename);
      }
    }
  }

  public void create(String tableName, Column[] columns) {
    try {
      lock.writeLock().lock();
      if (this.tableMap.containsKey(tableName))
        throw new DuplicateTableException(tableName);
      Table table = new Table(this.databaseName, tableName, columns);
      this.tableMap.put(tableName, table);
      this.persist();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Table get(String tableName) {
    try {
      lock.readLock().lock();
      if (!this.tableMap.containsKey(tableName))
        throw new TableNotExistException(tableName);
      return this.tableMap.get(tableName);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void drop(String tableName) {
    try {
      lock.writeLock().lock();
      if (!this.tableMap.containsKey(tableName))
        throw new TableNotExistException(tableName);
      Table table = this.tableMap.get(tableName);
      String filename = table.getTableMetaPath();
      File file = new File(filename);
      if (file.isFile() && !file.delete())
        throw new FileIOException(tableName + " _meta  when drop a table in database");

      table.dropTable();
      this.tableMap.remove(tableName);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void dropDatabase() {
    try {
      lock.writeLock().lock();
      for (Table table : this.tableMap.values()) {
        File file = new File(table.getTableMetaPath());
        if (file.isFile()&&!file.delete())
          throw new FileIOException(this.databaseName + " _meta when drop the database");
        table.dropTable();
      }
      this.tableMap.clear();
      this.tableMap = null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void recover() {
    System.out.println("! try to recover database " + this.databaseName);
    File tableFolder = new File(this.getDatabaseTableFolderPath());
    File[] files = tableFolder.listFiles();
    if (files == null) return;

    for (File file : files) {
      if (!file.isFile() || !file.getName().endsWith(Global.META_SUFFIX)) continue;
      try {
        String fileName = file.getName();
        String tableName = fileName.substring(0,fileName.length()-Global.META_SUFFIX.length());
        if (this.tableMap.containsKey(tableName))
          throw new DuplicateTableException(tableName);

        ArrayList<Column> columnList = new ArrayList<>();
        InputStreamReader reader = new InputStreamReader(new FileInputStream(file));
        BufferedReader bufferedReader = new BufferedReader(reader);
        String readLine;
        while ((readLine = bufferedReader.readLine()) != null)
          columnList.add(Column.parseColumn(readLine));
        bufferedReader.close();
        reader.close();
        Table table = new Table(this.databaseName, tableName, columnList.toArray(new Column[0]));
        System.out.println(table.toString());
        for(Row row: table)
          System.out.println(row.toString());
        this.tableMap.put(tableName, table);
      } catch (Exception ignored) {
      }
    }
  }

  public void quit() {
    try {
      this.lock.writeLock().lock();
      for (Table table : this.tableMap.values())
        table.persist();
      this.persist();
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  public void insert(String tableName, String[] columnNames, String[] values){
    Table table = get(tableName);
    ArrayList<Cell> cell_list = new ArrayList<Cell>();
    if (columnNames.length == 0) {
      int len = values.length;
      for (int i = 0; i < len; i++){
        cell_list.add(Column.parseEntry(values[i], table.columns.get(i)));
      }
    } else {
      int len = columnNames.length;
      if (len != values.length){
        throw new RuntimeException("The numbers of columns and values given don't match!");
      }
      for (Column column : table.columns){
        int index = -1;
        for (int i = 0; i < len; i++){
          if (column.getColumnName().equals(columnNames[i])){
            index = i;
          }
        }
        if (index == -1){
          cell_list.add(new Cell(null));
        }
        else{
          cell_list.add(Column.parseEntry(values[index], column));
        }
      }
    }
    table.insert(new Row(cell_list));
    table.persist();
  }

  public void insert_single_row(String tableName, String[] columnNames, String[] values){
    Table table = get(tableName);
    table.insert_single_row(columnNames, values);
  }

  public String update_rows(String table_name, String column_name, String value, Logic logic){
    Table table = get(table_name);
    return table.update_rows(column_name, value, logic);
  }

  public QueryResult select(QueryTable queryTable, String[] returnColumns, boolean isDistinct) {
    try {
      lock.readLock().lock();
      QueryResult queryResult = new QueryResult(queryTable, returnColumns, isDistinct);
      LinkedList<Row> rows = new LinkedList<>();
      while (queryTable.hasNext()) {
        rows.push(queryTable.next());
        queryResult.addRow(rows);
        rows.clear();
      }

      return queryResult;
    } finally {
      lock.readLock().unlock();
    }
  }

  public QueryTable getSingleQueryTable(String tableName) {
    try {
      lock.readLock().lock();
      if (tableMap.containsKey(tableName)) {
        return new QueryTable(tableMap.get(tableName));
      }
      else {
        throw new TableNotExistException(tableName);
      }
    }
    finally {
      lock.readLock().unlock();
    }
  }

  public QueryTable getMultiQueryTable(List<String > tableNames, Logic logic) {
    try {
      lock.readLock().lock();
      ArrayList<Table> multiTables = new ArrayList<>();
      for(String tableName : tableNames) {
        if (tableMap.containsKey(tableName)) {
          multiTables.add(tableMap.get(tableName));
        } else
          throw new TableNotExistsException(databaseName, tableName);
      }
      return new QueryTable(multiTables, logic);
    } finally {
      lock.readLock().unlock();
    }
  }




  // Find position
  public String getDatabasePath(){
    return Global.DBMS_DIR + File.separator + "data" + File.separator + this.databaseName;
  }
  public String getDatabaseTableFolderPath(){
    return this.getDatabasePath() + File.separator + "tables";
  }
  public String getDatabaseLogFilePath(){
    return this.getDatabasePath() + File.separator + "log";
  }
  public static String getDatabaseLogFilePath(String databaseName){
    return Global.DBMS_DIR + File.separator + "data" + File.separator + databaseName + File.separator + "log";
  }

  // Other utils.
  public String getDatabaseName() { return this.databaseName; }
  public String getTableInfo(String tableName) { return get(tableName).toString(); }
  public ArrayList<String> getTableNameList() {
    ArrayList<String> tableNames = new ArrayList<>();
    for (Map.Entry<String, Table> entry : tableMap.entrySet()) {
      tableNames.add(entry.getValue().tableName);
    }
    return tableNames;
  }
  public String toString() {
    if (this.tableMap.isEmpty()) return "{\n[DatabaseName: " + databaseName + "]\n" + Global.DATABASE_EMPTY + "}\n";
    StringBuilder result = new StringBuilder("{\n[DatabaseName: " + databaseName + "]\n");
    for (Table table : this.tableMap.values())
      if (table != null)
        result.append(table.toString());
    return result.toString() + "}\n";
  }

  public String delete(String tableName, Logic logic) {
    return get(tableName).delete(logic);
  }
}
