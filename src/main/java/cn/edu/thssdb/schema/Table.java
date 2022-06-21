package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.common.Global;
import cn.edu.thssdb.common.Pair;
import cn.edu.thssdb.query.Logic;
import cn.edu.thssdb.query.MultiRow;
import cn.edu.thssdb.type.BoolType;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static cn.edu.thssdb.type.ColumnType.STRING;


// TODO lock control, variables init.

public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  public BPlusTree<Cell, Row> index;
  private ArrayList<Long> xLocks;
  private ArrayList<Long> sLocks;
  private int primaryIndex;

  private boolean isPropertyModified;

  private int topLock;

  // ADD lock variables for S, X locks and etc here.

  // TODO: table/tuple level locks
  public Boolean testSLock(Long sessionId){ return false;}
  public void takeSLock(Long sessionId) {}
  public void releaseSLock(Long sessionId){}
  public Boolean testXLock(Long sessionId){ return false;}
  public Boolean takeXLock(Long sessionId){ return false;} // 在test成功前提下拿X锁。返回值false表示session之前已拥有这个表的X锁。
  public void releaseXLock(Long sessionId){}


  // Initiate: Table, recover
  public Table(String databaseName, String tableName, Column[] columns) {
    this.lock = new ReentrantReadWriteLock();
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList<>(Arrays.asList(columns));
    this.index = new BPlusTree<>();
    this.primaryIndex = -1;

    for (int i=0;i<this.columns.size();i++)
    {
      if(this.columns.get(i).isPrimary()){
        if(this.primaryIndex >= 0)
          throw new MultiPrimaryKeyException(this.tableName);
        this.primaryIndex = i;
      }
    }
    if(this.primaryIndex < 0)
      throw new NoPrimaryKeyException(this.tableName);

    // TODO initiate lock status.
    this.xLocks = new ArrayList<>();
    this.sLocks = new ArrayList<>();
    this.topLock = 0;
    isPropertyModified = true;

    recover();
  }

  private void recover() {
    // read from disk for recovering
      try {
        // TODO lock control
        ArrayList<Row> rowsOnDisk = deserialize();
        for(Row row: rowsOnDisk)
          this.index.put(row.getEntries().get(this.primaryIndex), row);
      }finally {
        // TODO lock control
      }
  }


  // Operations: get, insert, delete, update, dropTable, you can add other operations.
  // remember to use locks to fill the TODOs

  public Row get(Cell primaryCell){
    try {
      // TODO lock control
      return this.index.get(primaryCell);
    }finally {
      // TODO lock control
    }
  }

  public void insert(Row row) {
    try {
      // TODO lock control
      lock.writeLock().lock();
      this.checkRowValidInTable(row);
      if(this.containsRow(row))
        throw new DuplicateKeyException();
      this.index.put(row.getEntries().get(this.primaryIndex), row);
      this.isPropertyModified = true;
      }finally {
      // TODO lock control
      lock.writeLock().unlock();
    }
  }

  public void insert_single_row(String[] columnNames, String[] values){
    ArrayList<Cell> cell_list = new ArrayList<>();
    if (columnNames.length == 0) {
      int len = values.length;
      for (int i = 0; i < len; i++){
        cell_list.add(Column.parseEntry(values[i], columns.get(i)));
      }
    } else {
      int len = columnNames.length;
      if (len != values.length){
        throw new RuntimeException("The numbers of columns and values given don't match!");
      }
      for (Column column : columns){
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
    insert(new Row(cell_list));
    persist();
  }

  public void delete(Row row) {
    try {
      // TODO lock control.
      lock.writeLock().lock();
      isPropertyModified = true;
      this.checkRowValidInTable(row);
      if(!this.containsRow(row))
        throw new KeyNotExistException();
      this.index.remove(row.getEntries().get(this.primaryIndex));
    }finally {
      // TODO lock control.
      lock.writeLock().unlock();
    }
  }

  public String delete(Logic logic) {
    int cnt = 0;
    isPropertyModified = true;
    for (Row row : this) {
      MultiRow multiRow = new MultiRow(row, this);
      if (logic == null || logic.exec(multiRow) == BoolType.TRUE) {
        delete(row);
        cnt++;
      }
    }
    return "Deleted " + cnt + " items.";
  }

  public void update(Cell primaryCell, Row newRow) {
    try {
      // TODO lock control.
      this.checkRowValidInTable(newRow);
      Row oldRow = this.get(primaryCell);
      if(this.containsRow(newRow) && primaryCell.compareTo(newRow.getEntries().get(this.primaryIndex)) != 0)
        throw new DuplicateKeyException();   // 要么删并插入，要么抛出异常
      this.index.remove(primaryCell);
      this.index.put(newRow.getEntries().get(this.primaryIndex), newRow);
    }finally {
      // TODO lock control.
    }
  }

  public String update_rows(String column_name, String value, Logic logic){
    TableIterator it = (TableIterator) iterator();
    int update_count = 0;
    while(it.hasNext()){
      Row row = it.next();
      MultiRow multiRow = new MultiRow(row, this);
      if (logic == null || logic.exec(multiRow) == BoolType.TRUE){
        Cell primary_cell = row.getEntries().get(primaryIndex);
        Row new_row = new Row(row);
        int len = columns.size();
        int match_column = -1;
        for (int i = 0; i < len; i++){
          if (column_name.equals(columns.get(i).getColumnName())){
            match_column = i;
          }
        }
        if (match_column == -1){
          throw new AttributeNotFoundException(column_name);
        }
        Cell new_cell = Column.parseEntry(value, columns.get(match_column));
        new_row.getEntries().set(match_column, new_cell);
        update(primary_cell, new_row);
        update_count++;
      }
    }
    return "Updated " + update_count + " rows.";
  }

  private void serialize() {
    try {
      File tableFolder = new File(this.getTableFolderPath());
      if (!tableFolder.exists() ? !tableFolder.mkdirs() : !tableFolder.isDirectory())
        throw new FileIOException(this.getTableFolderPath() + " on serializing table in folder");
      File tableFile = new File(this.getTablePath());
      if (!tableFile.exists() ? !tableFile.createNewFile() : !tableFile.isFile())
        throw new FileIOException(this.getTablePath() + " on serializing table to file");
      FileOutputStream fileOutputStream = new FileOutputStream(this.getTablePath());
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
      for(Row row: this)
        objectOutputStream.writeObject(row);
      objectOutputStream.close();
      fileOutputStream.close();
    }catch (IOException e){
      throw new FileIOException(this.getTablePath() + " on serializing");
    }
  }

  private ArrayList<Row> deserialize() {
    try {
      File tableFolder = new File(this.getTableFolderPath());
      if (!tableFolder.exists() ? !tableFolder.mkdirs() : !tableFolder.isDirectory())
        throw new FileIOException(this.getTableFolderPath() + " when deserialize");
      File tableFile = new File(this.getTablePath());
      if(!tableFile.exists())
        return new ArrayList<>();
      FileInputStream fileInputStream = new FileInputStream(this.getTablePath());
      ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
      ArrayList<Row> rowsOnDisk = new ArrayList<>();
      Object tmpObj;
      while(fileInputStream.available() > 0){
        tmpObj = objectInputStream.readObject();
        rowsOnDisk.add((Row) tmpObj);
      }
      objectInputStream.close();
      fileInputStream.close();
      return rowsOnDisk;
    }catch (IOException e){
      throw new FileIOException(this.getTablePath() + " when deserialize");
    }catch (ClassNotFoundException e){
      throw new FileIOException(this.getTablePath() + " when deserialize(serialized object cannot be found)");
    }
  }

  public void persist(){
    try {
      // TODO add lock control.
      serialize();
    }
    finally {
      // TODO add lock control.
    }
  }

  public void dropTable(){ // remove table data file
    try {
      // TODO lock control.
      File tableFolder = new File(this.getTableFolderPath());
      if (!tableFolder.exists() ? !tableFolder.mkdirs() : !tableFolder.isDirectory())
        throw new FileIOException(this.getTableFolderPath() + " when dropTable");
      File tableFile = new File(this.getTablePath());
      if(tableFile.exists() && !tableFile.delete())
        throw new FileIOException(this.getTablePath() + " when dropTable");
    }
    finally {
      // TODO lock control.
    }
  }

  public String getTableName() {
    return tableName;
  }

  public ArrayList<Column> getColumns() {
    return columns;
  }

  public int getSLock(long session) {
    int flag = 0;
    if (topLock == 2) {
      if (xLocks.contains(session)) flag = 0;
      else flag = -1;
    } else
    if (topLock == 1) {
      if (sLocks.contains(session)) flag = 0;
      else {
        sLocks.add(session);
        topLock = 1;
        flag = 1;
      }
    } else
    if (topLock == 0) {
      sLocks.add(session);
      topLock = 1;
      flag = 1;
    }
    return flag;
  }

  public int getXLock(long session) {
    int flag = 0;
    if (topLock == 2) {
      if (xLocks.contains(session)) flag = 0;
      else flag = -1;
    } else
    if (topLock == 1) {
      flag = -1;
    } else
    if (topLock == 0) {
      xLocks.add(session);
      topLock = 2;
      flag = 1;
    }
    return flag;
  }

  public void freeSLock(long session) {
    if (sLocks.contains(session)) {
      sLocks.remove(session);
      if (sLocks.size() == 0) topLock = 0;
      else topLock = 1;
    }
  }

  public void freeXLock(long session) {
    if (xLocks.contains(session)) {
      topLock = 0;
      xLocks.remove(session);
    }
  }


  // Operations involving logic expressions.




  // Operations

  private class TableIterator implements Iterator<Row> {
    private Iterator<Pair<Cell, Row>> iterator;

    TableIterator(Table table) {
      this.iterator = table.index.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      return iterator.next().right;
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new TableIterator(this);
  }

  private void checkRowValidInTable(Row row){
    if(row.getEntries().size()!=this.columns.size())
      throw new SchemaLengthMismatchException(this.columns.size(), row.getEntries().size(), "when check Row Valid In table");
    for(int i=0;i<row.getEntries().size();i++) {
      String entryValueType = row.getEntries().get(i).getValueType();
      Column column = this.columns.get(i);
      if(entryValueType.equals(Global.ENTRY_NULL)){
        if(column.cantBeNull()) throw new NullValueException(column.getColumnName());
      }
      else{
        if (!entryValueType.equals(column.getColumnType().name()))
          throw new ValueFormatInvalidException("(when check row valid in table)");
        Comparable entryValue = row.getEntries().get(i).value;
        if(entryValueType.equals(STRING.name()) && ((String) entryValue).length()>column.getMaxLength())
          throw new ValueExceedException(column.getColumnName(), ((String) entryValue).length(), column.getMaxLength(), "(when check row valid in table)");
      }
    }
  }

  private Boolean containsRow(Row row){
    return this.index.contains(row.getEntries().get(this.primaryIndex));
  }

  public String getTableFolderPath(){
    return Global.DBMS_DIR + File.separator + "data" + File.separator + databaseName + File.separator + "tables";
  }
  public String getTablePath(){
    return this.getTableFolderPath() + File.separator + this.tableName;
  }
  public String getTableMetaPath(){
    return this.getTablePath() + Global.META_SUFFIX;
  }

  public String toString(){
    StringBuilder s = new StringBuilder("Table " + this.tableName + ": ");
    for (Column column : this.columns) s.append("\t(").append(column.toString()).append(')');
    return s.toString() + "\n";
  }

}
