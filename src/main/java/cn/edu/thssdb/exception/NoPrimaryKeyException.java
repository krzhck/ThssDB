package cn.edu.thssdb.exception;

public class NoPrimaryKeyException extends RuntimeException {
    private String name;

    public NoPrimaryKeyException(String name) {
        super();
        this.name = name;
    }
    @Override
    public String getMessage() {
        return "Exception: there is no primary keys in table " + name + "!";
    }
}
