package redisProtocol;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class OperationDetail {
    private Operation operation;
    private String value;
}
