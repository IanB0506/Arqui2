package gt.umg.centinela.simulator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Simulador de sensores urbanos.
 *
 * <p>Reemplaza a los dispositivos fisicos (camaras, sensores IoT, apps) emitiendo eventos con la
 * frecuencia, ubicacion y tipo que tendrian en una ciudad real. Puede ejecutarse en varias
 * maquinas a la vez (nodos C/D), cada una a cargo de un subconjunto de zonas.
 */
@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
public class SimulatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(SimulatorApplication.class, args);
    }
}
