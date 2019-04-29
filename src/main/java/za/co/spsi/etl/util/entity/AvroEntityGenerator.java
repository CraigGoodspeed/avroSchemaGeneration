package za.co.spsi.etl.util.entity;


import com.zaxxer.hikari.HikariDataSource;
import lombok.Data;
import lombok.SneakyThrows;
import za.co.spsi.etl.util.Avro.AvroTypeHelper;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

@Data
public class AvroEntityGenerator {

    private String table;
    private String select;
    private String output;
    private String fieldDelimiter;

    private final String outputType = ".schema";

    @SneakyThrows
    public List<AvroField> generateAvroEntities(HikariDataSource ds, HashMap<String, AvroTypeHelper> typeMapping){
        ArrayList<AvroField> toReturn = new ArrayList<>();
        try(PreparedStatement stat = ds.getConnection().prepareStatement(getSelect())){
            ResultSet rs = stat.executeQuery();
            while(rs.next()){
                AvroTypeHelper helper = typeMapping.get(rs.getString(3).toLowerCase());
                toReturn.add(
                        new AvroField(
                                parseFieldName(rs.getString(1)),
                                (Boolean)rs.getObject(2),
                                helper.getTargetType(),
                                helper.getLogicalType(),
                                (Short)rs.getObject(4),
                                (Integer)rs.getObject(5))
                );
            }
        }
        return toReturn;
    }

    public static void main(String[] args){
        AvroEntityGenerator gen = new AvroEntityGenerator();
        gen.setFieldDelimiter("_");
        gen.parseFieldName("Account_ID");
    }

    private String parseFieldName(String fieldName){
        if(fieldName.contains(fieldDelimiter)) {
            String toReturn = Arrays.stream(fieldName.toLowerCase().split(fieldDelimiter)).map(
                    s -> s.substring(0, 1).toUpperCase().concat(s.substring(1))

            ).reduce("", String::concat);
            return toReturn.substring(0, 1).toLowerCase().concat(toReturn.substring(1));
        }
        return fieldName.toLowerCase();
    }
}
