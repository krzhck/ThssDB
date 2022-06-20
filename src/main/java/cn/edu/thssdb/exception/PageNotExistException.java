package cn.edu.thssdb.exception;

public class PageNotExistException extends Exception{
    @Override
    public String getMessage() {
        return "The page is not exist.";
    }
}
