package gt.umg.centinela.alerts.persistence;

import gt.umg.centinela.common.json.Json;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Map;

/** Bitacora de auditoria: toda accion humana o purga de datos personales queda registrada. */
@Repository
public class AuditRepository {

    private final JdbcTemplate jdbc;

    public AuditRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void record(String actor, String action, String target, Map<String, ?> detail) {
        jdbc.update("INSERT INTO audit_log (actor, action, target, detail) VALUES (?, ?, ?, ?::jsonb)",
                actor, action, target, Json.write(detail));
    }
}
