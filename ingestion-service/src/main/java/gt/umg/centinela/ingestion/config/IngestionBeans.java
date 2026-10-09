package gt.umg.centinela.ingestion.config;

import gt.umg.centinela.common.privacy.PlatePseudonymizer;
import gt.umg.centinela.ingestion.pipeline.EventNormalizer;
import jakarta.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration
public class IngestionBeans {

    private static final Logger log = LoggerFactory.getLogger(IngestionBeans.class);

    @Bean
    public PlatePseudonymizer platePseudonymizer(IngestionProperties properties) {
        if (properties.plateHmacSecret().startsWith("dev-")) {
            log.warn("Se esta usando la llave HMAC de desarrollo. Defina PLATE_HMAC_SECRET en produccion.");
        }
        return new PlatePseudonymizer(properties.plateHmacSecret());
    }

    @Bean
    public EventNormalizer eventNormalizer(IngestionProperties properties, PlatePseudonymizer pseudonymizer,
                                           Validator validator) {
        return new EventNormalizer(properties, pseudonymizer, validator, Clock.systemUTC());
    }
}
