package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.MetaInfo;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.BoolType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

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
//  protected Queue<Row> buf;


  public QueryTable(Table table) {
    // TODO
    this.tables = new ArrayList<>();
    this.tables.add(table);
  }

  public QueryTable(ArrayList<Table> tables, Logic logic) {
    this.tables = new ArrayList<>(tables);
    this.setLogic(logic);
  }

  boolean isSet = false;

  public void setLogic(Logic logic) {
    currentRow = new ArrayList<>();
    if (this.joinLogic == null) {
      this.joinLogic = logic;
    }
    else if (this.selectLogic == null) {
      this.selectLogic = logic;
    }
  }

  void prepareSatisfyNext() {
    while (getNext()) {
      boolean InSelect = selectLogic == null || selectLogic.exec(new MultiRow(currentRow, tables)) == BoolType.TRUE;
      boolean InJoin = joinLogic == null || joinLogic.exec(new MultiRow(currentRow, tables)) == BoolType.TRUE;
      if (InSelect && InJoin) {
        return;
      }
    }
  }

  boolean isEmpty = false;

  boolean getNext() {
    checkFirstSet();
    if (isEmpty) {
      return false;
    }
    if (currentRow.size() == 0) {
      for (Table table : tables) {
        iterators.add(table.iterator());
      }
      for(int i = 0; i < iterators.size(); i++) {
        if (!iterators.get(i).hasNext()) {
          isEmpty = true;
          return false;
        } else {
          currentRow.add(iterators.get(i).next());
        }
      }
      return true;
    }
    assert iterators.size() == tables.size() && tables.size() == currentRow.size();
    int i = (int) iterators.size() - 1;
    for (; i >= 0; i--) {
      currentRow.remove(i);
      if (iterators.get(i).hasNext())
        break;
      else
        iterators.set(i, tables.get(i).iterator());
    }
    if (i < 0) {
      isEmpty = true;
      return false;
    }
    for(; i < iterators.size(); i ++) {
      currentRow.add(iterators.get(i).next());
    }
    assert iterators.size() == tables.size() && tables.size() == currentRow.size();
    return true;
  }

  void checkFirstSet() {
    if (!isSet) {
      isSet = true;
      prepareSatisfyNext();
    }
  }


  @Override
  public boolean hasNext() {
    // TODO
    checkFirstSet();
    return !isEmpty;
  }

//  @Override
//  public Row next() {
//    // TODO
//    return null;
//  }

  @Override
  public MultiRow next() {
    MultiRow ret = new MultiRow(currentRow, tables);
    prepareSatisfyNext();
    return ret;
  }
}