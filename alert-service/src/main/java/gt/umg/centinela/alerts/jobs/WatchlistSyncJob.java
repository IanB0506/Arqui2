package gt.umg.centinela.alerts.jobs;

import gt.umg.centinela.alerts.config.AlertServiceProperties;
import gt.umg.centinela.alerts.persistence.WatchlistRepository;
import gt.umg.centinela.common.privacy.PlatePseudonymizer;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Tarea periodica: copia los seudonimos de la lista de placas robadas desde PostgreSQL (fuente de
 * verdad) al set de Redis que consulta el motor de correlacion en cada lectura LPR.
 */
@Component
public class WatchlistSyncJob {

    private static final Logger log = LoggerFactory.getLogger(WatchlistSyncJob.class);
    static final String REDIS_KEY = "watchlist:plates";

    private final WatchlistRepository repository;
    private final StringRedisTemplate redis;
    private final PlatePseudonymizer pseudonymizer;
    private final AlertServiceProperties properties;

    public WatchlistSyncJob(WatchlistRepository repository, StringRedisTemplate redis, PlatePseudonymizer pseudonymizer,
                            AlertServiceProperties properties) {
        this.repository = repository;
        this.redis = redis;
        this.pseudonymizer = pseudonymizer;
        this.properties = properties;
    }

    /** Al arrancar carga la lista inicial (configuracion) y sincroniza. */
    @EventListener(ApplicationReadyEvent.class)
    public void seed() {
        for (String plate : properties.watchlistPlates()) {
            repository.add(pseudonymizer.pseudonymizePlate(plate), PlatePseudonymizer.maskPlate(plate),
                    "Reporte de robo (carga inicial)", "sistema");
        }
        sync();
    }

    @Scheduled(fixedDelay = 600_000, initialDelay = 600_000)
    @SchedulerLock(name = "watchlist-sync", lockAtMostFor = "PT2M")
    public void scheduledSync() {
        sync();
    }

    public void sync() {
        try {
            List<String> hashes = repository.allHashes();
            redis.delete(REDIS_KEY);
            if (!hashes.isEmpty()) {
                redis.opsForSet().add(REDIS_KEY, hashes.toArray(String[]::new));
            }
            log.info("Lista de placas robadas sincronizada en Redis: {} placas", hashes.size());
        } catch (RuntimeException e) {
            log.warn("No se pudo sincronizar la lista de placas (se reintentara): {}", e.getMessage());
        }
    }
}
