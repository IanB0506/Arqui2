package gt.umg.centinela.correlation;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
public class CorrelationApplication {
    public static void main(String[] args) {
        SpringApplication.run(CorrelationApplication.class, args);
    }
}
