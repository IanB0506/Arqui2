package gt.umg.centinela.correlation;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Motor de correlacion e inferencia de eventos.
 *
 * <p>El estado (ventanas de tiempo, incidentes abiertos, sospechosos) vive en Redis y no en la
 * memoria del proceso: por eso pueden correr varias instancias en distintas maquinas y cualquiera
 * puede caerse sin perder el contexto. Kafka reparte las zonas (llave del mensaje) entre instancias.
 */
@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
public class CorrelationApplication {

    public static void main(String[] args) {
        SpringApplication.run(CorrelationApplication.class, args);
    }
}
