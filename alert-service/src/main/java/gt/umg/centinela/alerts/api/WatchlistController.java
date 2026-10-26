package gt.umg.centinela.alerts.api;

import gt.umg.centinela.alerts.jobs.WatchlistSyncJob;
import gt.umg.centinela.alerts.persistence.AuditRepository;
import gt.umg.centinela.alerts.persistence.WatchlistRepository;
import gt.umg.centinela.common.privacy.PlatePseudonymizer;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Lista de vehiculos con reporte de robo. El administrador envia la placa en claro (p. ej. desde un
 * oficio de la PNC); se seudonimiza al instante y la placa en claro no se guarda en ningun lado.
 */
@RestController
@RequestMapping("/api/watchlist")
public class WatchlistController {

    private final WatchlistRepository repository;
    private final PlatePseudonymizer pseudonymizer;
    private final AuditRepository audit;
    private final WatchlistSyncJob sync;

    public WatchlistController(WatchlistRepository repository, PlatePseudonymizer pseudonymizer,
                               AuditRepository audit, WatchlistSyncJob sync) {
        this.repository = repository;
        this.pseudonymizer = pseudonymizer;
        this.audit = audit;
        this.sync = sync;
    }

    public record WatchlistRequest(@NotBlank @Pattern(regexp = "^[A-Za-z0-9 -]{4,12}$") String plate,
                                   @NotBlank String reason) {
    }

    @GetMapping
    public List<Map<String, Object>> list() {
        return repository.listMasked();
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> add(@Valid @RequestBody WatchlistRequest request, Authentication auth) {
        String hash = pseudonymizer.pseudonymizePlate(request.plate());
        String masked = PlatePseudonymizer.maskPlate(request.plate());
        boolean added = repository.add(hash, masked, request.reason(), auth.getName());
        audit.record(auth.getName(), "AGREGAR_PLACA_LISTA", masked, Map.of("reason", request.reason()));
        sync.sync();
        return ResponseEntity.status(added ? HttpStatus.CREATED : HttpStatus.OK)
                .body(Map.of("plate_masked", masked, "added", added));
    }

    @DeleteMapping("/{plate}")
    public ResponseEntity<Void> remove(@PathVariable String plate, Authentication auth) {
        boolean removed = repository.remove(pseudonymizer.pseudonymizePlate(plate));
        audit.record(auth.getName(), "QUITAR_PLACA_LISTA", PlatePseudonymizer.maskPlate(plate), Map.of("removed", removed));
        sync.sync();
        return removed ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }
}
