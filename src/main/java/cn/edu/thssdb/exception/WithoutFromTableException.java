package cn.edu.thssdb.exception;

public class WithoutFromTableException extends RuntimeException{
    @Override
    public String getMessage() {
        return "Exception occurs : Select Option WithOut Database";
    }
}
