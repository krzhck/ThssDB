package cn.edu.thssdb.query;


import cn.edu.thssdb.schema.Cell;
import cn.edu.thssdb.schema.MetaInfo;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.QueryResultType;

import java.util.*;
import java.util.function.Predicate;

/**
 * Designed to hold general query result:
 * In SQL result, the returned answer could be QueryTable OR an error message
 * For errors, resultType = QueryResultType.MESSAGE, see Construct method.
 * For results, it will hold QueryTable.
 */

public class QueryResult {

  public final QueryResultType resultType;
  public final String errorMessage; // If it is an error.

  private List<MetaInfo> metaInfos;
  private List<String> columnNames;

  public List<Row> results;

  private List<Integer> indices;
  private Predicate<Row> predicate;

  public String[] cNames;
  public boolean distinct;
  public boolean wildcard = false;

  private boolean onlyMessage=false;
  private QueryTable queryTable;
  private HashSet<String> hashSet = new HashSet<>();


  public QueryResult(QueryTable queryTable, String[] chooseColumns, boolean distinct) {
    this.resultType = QueryResultType.SELECT;
    this.errorMessage = null;
    this.results = new ArrayList<>();
    this.distinct = distinct;
    this.cNames = chooseColumns;
    indices = new ArrayList<>();
    this.queryTable = queryTable;
    this.columnNames = new ArrayList<>();
    initIndex(chooseColumns);

//    if (cNames == null) {
//      wildcard = true;
//    }
//    else {
//      initIndex(chooseColumns);
////      for (String name : cNames) {
////        MultiRow row = new MultiRow();
////        int index = row.getColumnIndex(name);
////        indices.add(index);
////      }
//    }
  }

  void initIndex(String[] chooseColumns) {
    indices = new ArrayList<>();
    if (chooseColumns == null || chooseColumns.length == 0) {
      int count = 0;
      for(Table table : queryTable.tables) {
        for(int i = 0; i < table.getColumns().size(); i++) {
          indices.add(count ++);
          columnNames.add(table.getColumns().get(i).getColumnName());
        }
      }
    } else {
      MultiRow temp = new MultiRow(null, queryTable.tables);
      for (String i : chooseColumns) {
        indices.add(temp.getColumnIndex(i));
        columnNames.add(i);
      }
    }
  }

  public QueryResult(QueryTable[] queryTables) {
    this.resultType = QueryResultType.SELECT;
    this.errorMessage = null;
    // TODO
  }

  public QueryResult(String errorMessage){
    resultType = QueryResultType.MESSAGE;
    this.errorMessage = errorMessage;
  }

  public void addRow(LinkedList<Row> rows) {
    Row row = QueryResult.combineRow(rows);
    row = generateQueryRecord(row);
    if (distinct) {
      if (!results.contains(row)) {
        results.add(row);
      }
    }
    else {
      results.add(row);
    }
//    if (!orderIndices.isEmpty()) {
//      results.sort((o1, o2) -> {
//        for (Integer i : orderIndices) {
//          int cmp = o1.getEntries().get(i).compareTo(o2.getEntries().get(i));
//          if (cmp > 0) {
//            return order ? 1 : -1;
//          }
//          else if (cmp < 0) {
//            return order ? -1 : 1;
//          }
//        }
//        return 0;
//      });
//    }
  }

  public static Row combineRow(LinkedList<Row> rows) {
    // TODO
    Row r = new Row();
    for (int i = rows.size() - 1; i >= 0; i--) {
      r.appendEntries(rows.get(i).getEntries());
    }
    return r;
  }

  public Row generateQueryRecord(Row row) {
    // TODO
    if (wildcard) {
      return row;
    }
    ArrayList<Cell> record = new ArrayList<>();
    for (int i : indices) {
      record.add(row.getEntries().get(i));
    }
    return new Row(record.toArray(new Cell[indices.size()]));
  }

  public List<String> getColumnNames(){return this.columnNames;}
}
