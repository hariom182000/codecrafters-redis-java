package redisProtocol;


public class OperationDetail {
    private Operation operation;
    private String value;

    public OperationDetail(final Operation operation, final String value) {
        this.operation = operation;
        this.value = value;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setOperation(final Operation operation) {
        this.operation = operation;
    }

    public String getValue() {
        return value;
    }

    public void setValue(final String value) {
        this.value = value;
    }
}