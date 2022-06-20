package cn.edu.thssdb.exception;

public class AttributeNotFoundException extends RuntimeException {
    private String attribute_name;

    public AttributeNotFoundException(String attribute_name){
        this.attribute_name = attribute_name;
    }

    @Override
    public String getMessage() {
        return "Exception occurs: Attribute " + attribute_name + " Not Found!";
    }

}
