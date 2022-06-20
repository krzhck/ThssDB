package cn.edu.thssdb.exception;

public class ParseStringColumnException extends RuntimeException {
    private String errorMessage;
    public ParseStringColumnException(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @Override
    public String getMessage() {
        return errorMessage;
    }
}
