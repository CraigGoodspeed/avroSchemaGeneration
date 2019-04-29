package za.co.spsi.etl.util.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class AvroField {
    String fieldName;
    Boolean nullable;
    String dataType;
    String logicalType;
    Short precision;
    Integer scale;


    private final String nonNullableBasicType = "{\"name\":\"%s\", \"type\":\"%s\"}";
    private final String nullableBasicType = "{\"name\":\"%s\", \"type\":[\"null\",\"%s\"]}";
    private final String nullableLogicalType =  "{\"name\":\"%s\", \"type\":[\"null\",{\"type\":\"%s\", \"logicalType\" : \"%s\" } ], \"default\":null}";
    private final String nullableLogicalTypePrecisionScale =  "{\"name\":\"%s\", \"type\":[\"null\",{\"type\":\"%s\", \"logicalType\" : \"%s\", \"precision\":%s, \"scale\":%s } ], \"default\":null}";


    @Override
    public String toString(){
        //basic type, this is simple.
        if(logicalType == null){
            String toFormat = getNullable() ? nullableBasicType : nonNullableBasicType;
            return String.format(toFormat, getFieldName(),getDataType());
        }
        else{
            if(getPrecision() == null && getScale() == null)
                return String.format(nullableLogicalType, getFieldName(), getDataType(), getLogicalType());
            return String.format(nullableLogicalTypePrecisionScale, getFieldName(), getDataType(), getLogicalType(), getPrecision(), getScale());
        }
    }

}
