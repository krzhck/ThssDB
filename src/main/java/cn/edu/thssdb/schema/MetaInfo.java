package cn.edu.thssdb.schema;

import cn.edu.thssdb.schema.Column;
import java.util.ArrayList;
import java.util.List;

/**
 * MetaInfo is used to hold and index meta information.
 */

public class MetaInfo {

  private final String tableName;
  private final List<Column> columns;

  public MetaInfo(String tableName, ArrayList<Column> columns) {
    this.tableName = tableName;
    this.columns = columns;
  }

  public int columnFind(String name) {
    int size = columns.size();
    for (int i = 0; i < size; ++i)
      if (columns.get(i).getColumnName().equals(name))
        return i;
    return -1;
  }

  String getColumnName(int index) {
    if (index < 0 || index >= columns.size())
      return null;
    return columns.get(index).getColumnName();
  }

  String getTableDotColumnName(int index) {
    if (index < 0 || index >= columns.size())
      return null;
    return tableName + "." + getColumnName(index);
  }

  public int getColumnSize() {
    return columns.size();
  }

  public String getTableName() {
    return tableName;
  }

  public Column getIndexColumn(int index) {
    if (index < 0 || index > (columns.size() - 1)) {
      return null;
    }
    return columns.get(index);
  }
}