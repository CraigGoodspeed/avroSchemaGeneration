package za.co.spsi.etl.util;

import java.io.InputStream;

public class ResourceFile{

    String filePath;
    public ResourceFile(String filePath){
        this.filePath = filePath;
    }

    public InputStream openStream(){
        ClassLoader classLoader = getClass().getClassLoader();
        return classLoader.getResourceAsStream(filePath);
    }
}
