package za.co.spsi.etl.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import za.co.spsi.etl.util.Avro.AvroGenerator;
import za.co.spsi.etl.util.beans.myDataSource;


@SpringBootApplication
public class startup implements CommandLineRunner {
    @Value("${avro.format}")
    private String AVROFORMAT;

    public static Logger LOG = LoggerFactory
            .getLogger(startup.class);
    public static void main(String[] args){
        SpringApplication.run(startup.class,args);
    }

    @Autowired
    myDataSource ds;
    @Autowired
    private ConfigurableApplicationContext context;

    @Override
    public void run(String... args) throws Exception {
        String outputDirectory = args[0];
        LOG.info("starting the runner");
        LOG.info("starting up avro generator");
        for(String s : args){
            LOG.info(s);
        }
        AvroGenerator ag = new AvroGenerator(this.ds.createConnection(), AVROFORMAT, outputDirectory);
        ag.init();
        LOG.info("avro generator completed.");
        SpringApplication.exit(context);
    }
}
