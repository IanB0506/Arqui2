package gt.umg.centinela.alerts.persistence;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/** Lista de vehiculos con reporte de robo (solo seudonimos y version enmascarada). */
@Repository
public class WatchlistRepository {

    private final JdbcTemplate jdbc;

    public WatchlistRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public boolean add(String plateHash, String plateMasked, String reason, String addedBy) {
        return jdbc.update("""
                INSERT INTO plate_watchlist (plate_hash, plate_masked, reason, added_by) VALUES (?, ?, ?, ?)
                ON CONFLICT (plate_hash) DO NOTHING
                """, plateHash, plateMasked, reason, addedBy) == 1;
    }

    public boolean remove(String plateHash) {
        return jdbc.update("DELETE FROM plate_watchlist WHERE plate_hash = ?", plateHash) == 1;
    }

    public List<String> allHashes() {
        return jdbc.queryForList("SELECT plate_hash FROM plate_watchlist", String.class);
    }

    public List<Map<String, Object>> listMasked() {
        return jdbc.queryForList("SELECT plate_masked, reason, added_at, added_by FROM plate_watchlist ORDER BY added_at DESC");
    }
}
