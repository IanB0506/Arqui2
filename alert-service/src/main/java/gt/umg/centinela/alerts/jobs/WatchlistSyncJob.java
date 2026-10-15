package gt.umg.centinela.alerts.jobs;

import gt.umg.centinela.alerts.config.AlertServiceProperties;
import gt.umg.centinela.alerts.persistence.WatchlistRepository;
import gt.umg.centinela.common.RedisKeys;
import gt.umg.centinela.common.privacy.PlatePseudonymizer;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * Copia los seudonimos de la lista de placas robadas desde PostgreSQL (fuente de verdad) al set de
 * Redis que consulta el motor de correlacion en cada lectura LPR.
 *
 * <p>Redis no persiste (ADR-004): si el nodo A se reinicia, vuelve vacio. Por eso, al cargar la lista
 * se escribe una llave centinela ({@code watchlist:synced}); cada 15 s se revisa que siga ahi y, si
 * falta, se recargan de inmediato la lista y los ajustes de umbral. Antes la lista quedaba vacia hasta
 * 10 min y un vehiculo robado podia pasar sin alerta.
 */
@Component
public class WatchlistSyncJob {

    private static final Logger log = LoggerFactory.getLogger(WatchlistSyncJob.class);
    static final String REDIS_KEY = RedisKeys.WATCHLIST;
    private static final String TMP_KEY = RedisKeys.WATCHLIST + ":tmp";

    private final WatchlistRepository repository;
    private final StringRedisTemplate redis;
    private final PlatePseudonymizer pseudonymizer;
    private final AlertServiceProperties properties;
    private final RuleQualityJob ruleQuality;

    public WatchlistSyncJob(WatchlistRepository repository, StringRedisTemplate redis, PlatePseudonymizer pseudonymizer,
                            AlertServiceProperties properties, RuleQualityJob ruleQuality) {
        this.repository = repository;
        this.redis = redis;
        this.pseudonymizer = pseudonymizer;
        this.properties = properties;
        this.ruleQuality = ruleQuality;
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

    /** Detecta que Redis volvio vacio (falta la centinela) y recarga sin esperar al ciclo de 10 min. */
    @Scheduled(fixedDelay = 15_000, initialDelay = 15_000)
    @SchedulerLock(name = "watchlist-sentinel", lockAtMostFor = "PT10S")
    public void checkSentinel() {
        try {
            if (Boolean.TRUE.equals(redis.hasKey(RedisKeys.WATCHLIST_SYNCED))) {
                return;
            }
        } catch (RuntimeException e) {
            return; // Redis caido: se revisa en la siguiente ronda
        }
        log.warn("Redis no tiene la lista de placas robadas (se reinicio vacio): se recargan la lista y los ajustes de umbral");
        if (sync()) {
            try {
                log.info("Ajustes de umbral recargados en Redis: {}", ruleQuality.republishFromDatabase());
            } catch (RuntimeException e) {
                log.warn("No se pudieron recargar los ajustes de umbral: {}", e.getMessage());
            }
        }
    }

    /** Reemplaza el set de forma atomica (se arma en una llave temporal y se renombra). */
    public boolean sync() {
        try {
            List<String> hashes = repository.allHashes();
            if (hashes.isEmpty()) {
                redis.delete(REDIS_KEY);
            } else {
                redis.delete(TMP_KEY);
                redis.opsForSet().add(TMP_KEY, hashes.toArray(String[]::new));
                redis.rename(TMP_KEY, REDIS_KEY);
            }
            redis.opsForValue().set(RedisKeys.WATCHLIST_SYNCED, Instant.now().toString());
            log.info("Lista de placas robadas sincronizada en Redis: {} placas", hashes.size());
            return true;
        } catch (RuntimeException e) {
            log.warn("No se pudo sincronizar la lista de placas (se reintentara): {}", e.getMessage());
            return false;
        }
    }
}
