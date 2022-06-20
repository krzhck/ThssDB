package cn.edu.thssdb.exception;

public class CompareDifferentTypeException extends RuntimeException{
    String name;
    public CompareDifferentTypeException(String name) {
        this.name = name;
    }

    @Override
    public String getMessage() {
        return "CompareDifferentTypeException: " + name ;
    }
}
