package cn.edu.thssdb.exception;

public class AssignDifferentTypeException extends RuntimeException{
    String name, value;
    public AssignDifferentTypeException(String name, String value) {
        this.name = name;
        this.value = value;
    }

    @Override
    public String getMessage() {
        return "AssignDifferentTypeException: " + name + "=" + value ;
    }
}
