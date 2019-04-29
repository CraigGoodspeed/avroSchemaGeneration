package za.co.spsi.etl.util.Avro;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import za.co.spsi.etl.util.ResourceFile;
import za.co.spsi.etl.util.entity.AvroEntityGenerator;
import za.co.spsi.etl.util.entity.AvroField;

import javax.annotation.PostConstruct;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;



public class AvroGenerator {

    private final HikariDataSource ds;
    private String AVROFORMAT;
    private String outputDirectory;

    public AvroGenerator(HikariDataSource ds, String avroFormat, String outputDirectory){
        this.ds = ds;
        this.AVROFORMAT = avroFormat;
        this.outputDirectory = outputDirectory;
    }

    @PostConstruct
    @SneakyThrows
    public void init(){
        ResourceFile schema = new ResourceFile("AvroConfiguration/avroSchema.json");
        HashMap<String, AvroTypeHelper> typeMapper = generateHashMap(new ResourceFile("AvroConfiguration/fieldTypeHashMap.json"));
        try(InputStream inputStream = schema.openStream()){
            ObjectMapper mapper = new ObjectMapper();
            AvroEntityGenerator[] toCreate = mapper.readValue(inputStream, AvroEntityGenerator[].class);
            Arrays.stream(toCreate).forEach(
                    t -> {
                        List<AvroField> fields = t.generateAvroEntities(ds, typeMapper);
                        String fieldData = Arrays.stream(fields.toArray()).map( f -> f.toString()).collect(Collectors.joining(","));
                        writeSchema(t,fieldData);
                    }
            );
        }
        this.ds.close();
    }

    @SneakyThrows
    private HashMap<String,AvroTypeHelper> generateHashMap(ResourceFile typeMapping){
        HashMap<String, AvroTypeHelper> toReturn = new HashMap<>();
        try(InputStream inStream = typeMapping.openStream()){
            ObjectMapper mapper = new ObjectMapper();
            AvroTypeHelper[] types = mapper.readValue(inStream, AvroTypeHelper[].class);
            Arrays.stream(types).forEach(
                    s -> toReturn.put(s.getSourceType().toLowerCase(),s)
            );
        }
        return toReturn;
    }
    @SneakyThrows
    private void writeSchema(AvroEntityGenerator t, String fieldData){

        try(FileWriter fw = new FileWriter(String.format("%s%s%s",(outputDirectory.endsWith("/") ? outputDirectory : String.format("%s%s",outputDirectory,"/")), t.getTable(),".avro"))) {
            fw.write(String.format(AVROFORMAT, t.getOutput(), t.getTable(), fieldData));
            fw.flush();
        }
    }
}
