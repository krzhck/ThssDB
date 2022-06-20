package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.AssignDifferentTypeException;
import cn.edu.thssdb.exception.AttributeNotFoundException;
import cn.edu.thssdb.exception.MultiPrimaryKeyException;
import cn.edu.thssdb.schema.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/*
* Rows = Row + Row + ... + Row
* */
public class MultiRow extends Row {
    ArrayList<Table> tableInfo; // tableInfo用于区分不同的列
    public List<MetaInfo> metaInfoInfos = new ArrayList<>();
    public MultiRow() {
        super();
        tableInfo = new ArrayList<>();
        entries = new ArrayList<>();
    }

    public void deepCopy(List<Cell> src) {
        Cell[] temp = new Cell[src.size()];
        for (int i = 0; i < src.size(); i++) {
            temp[i] = src.get(i);
        }
        entries = new ArrayList<Cell>(Arrays.asList(temp));
    }



    public MultiRow(Row row, Table table) {
        super();
        tableInfo = new ArrayList<>();
        tableInfo.add(table);
        this.entries = new ArrayList<>();
        this.entries.addAll(row.getEntries());
        metaInfoInfos.add(new MetaInfo(table.getTableName(), table.getColumns()));
    }

    public MultiRow(List<Row> rows, List<Table> tables) {
        super();
        tableInfo = new ArrayList<>(tables);
        this.entries = new ArrayList<>();
        for(Table table : tables) {
            metaInfoInfos.add(new MetaInfo(table.getTableName(), table.getColumns()));
        }
        if (rows != null)
        for(Row row : rows) {
            this.entries.addAll(row.getEntries());
        }
    }

    public void setValue(String columnName, String value) {
        int index = getColumnIndex(columnName);
        Column column = getColumnType(index);
        try {
            entries.set(index, new Cell(column.parseColumn(value)));
        } catch (Exception e) {
            throw new AssignDifferentTypeException(columnName, value);
        }
    }

    public int getColumnIndex(String target) {
        if (target.contains(".")) {
            String[] targetList = target.split("\\.");
            if (targetList.length != 2) {
                throw new AttributeNotFoundException(target);
            }
            int count = 0;
            for (MetaInfo metaInfo : metaInfoInfos) {
                if (metaInfo.getTableName().equals(targetList[0])) {
                    int find = metaInfo.columnFind(targetList[1]);
                    if (find >= 0) {
                        return find + count;
                    }
                }
                count += metaInfo.getColumnSize();
            }
            throw new AttributeNotFoundException(target);
        } else {
            int ret = -1;
            int count = 0;
            for(MetaInfo metaInfo : metaInfoInfos) {
                int find = metaInfo.columnFind(target);
                if (find >= 0) {
                    if (ret != -1) {
                        throw new MultiPrimaryKeyException(target);
                    }
                    ret = find + count;
                }
                count += metaInfo.getColumnSize();
            }
            if (ret == -1) {
                throw new AttributeNotFoundException(target);
            } else {
                return ret;
            }
        }
    }

    public Column getColumnType(int index) {
        for(MetaInfo metaInfo : metaInfoInfos) {
            if (metaInfo.getColumnSize() > index) {
                return metaInfo.getIndexColumn(index);
            } else {
                index -= metaInfo.getColumnSize();
            }
        }
        return null;
    }


}
