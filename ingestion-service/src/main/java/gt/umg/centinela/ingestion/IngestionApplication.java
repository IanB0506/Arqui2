package gt.umg.centinela.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Servicio de ingesta. Es la "puerta de entrada" del sistema: todo evento pasa por aqui antes de
 * llegar al motor de correlacion o a la base de datos. Es stateless, por lo que pueden correr
 * varias instancias en distintas maquinas: Kafka reparte las particiones entre ellas.
 */
@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
public class IngestionApplication {

    public static void main(String[] args) {
        SpringApplication.run(IngestionApplication.class, args);
    }
}
