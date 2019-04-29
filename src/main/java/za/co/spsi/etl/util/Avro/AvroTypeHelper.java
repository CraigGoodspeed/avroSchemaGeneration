package za.co.spsi.etl.util.Avro;

import lombok.Data;

@Data
public class AvroTypeHelper {
    String sourceType;
    String targetType;
    String logicalType;
}
