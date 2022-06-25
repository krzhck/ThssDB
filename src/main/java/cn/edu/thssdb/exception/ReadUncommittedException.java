package cn.edu.thssdb.exception;

public class ReadUncommittedException extends RuntimeException{
    private String table_name;

    public ReadUncommittedException(String m) { this.table_name = m; }
    @Override
    public String getMessage() {
        return "Exception: " + table_name + " has uncommitted changes!";
    }
}
