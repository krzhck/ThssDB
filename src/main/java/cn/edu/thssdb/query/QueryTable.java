package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.MetaInfo;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Designed for the select query with join/filtering...
 * hasNext() looks up whether the select result contains a next row
 * next() returns a row, plz keep an iterator.
 */

public class QueryTable implements Iterator<Row> {

  ArrayList<Iterator<Row>> iterators = new ArrayList<>();
  ArrayList<Table> tables;
  ArrayList<Row> currentRow;
  Logic selectLogic = null;
  Logic joinLogic = null;
//  public ArrayList<MetaInfo> genMetaInfo() {return null;}

  public QueryTable(Table table) {
    // TODO
    this.tables = new ArrayList<>();
    this.tables.add(table);
  }

  public QueryTable(ArrayList<Table> tables, Logic logic) {
    this.tables = new ArrayList<>(tables);
    this.setLogic(logic);
  }

  @Override
  public boolean hasNext() {
    // TODO
    return true;
  }

  @Override
  public Row next() {
    // TODO
    return null;
  }

  public void setLogic(Logic logic) {
    currentRow = new ArrayList<>();
    if (this.joinLogic == null) {
      this.joinLogic = logic;
    }
    else if (this.selectLogic == null) {
      this.selectLogic = logic;
    }
  }
}