package za.co.spsi.etl.util.beans;


import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class myDataSource {

    @Value("${db.etl.connectionString}")
    String jdbcConnection;
    @Value("${db.etl.username}")
    String username;
    @Value("${db.etl.password}")
    String password;

    @Bean
    public HikariDataSource createConnection(){
        HikariDataSource toReturn = new HikariDataSource();
        toReturn.setJdbcUrl(jdbcConnection);
        toReturn.setUsername(username);
        toReturn.setPassword(password);
        return toReturn;
    }
}
