package cn.edu.thssdb.exception;

public class TableNotExistsException extends RuntimeException{
    String databaseName, tableName;
    public TableNotExistsException(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    @Override
    public String getMessage() {
        return "Exception: table " + tableName + " from database " + databaseName + " do not exists.";
    }
}
