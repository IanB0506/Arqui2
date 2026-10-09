package gt.umg.centinela.common.geo;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Catalogo de las 12 zonas de la ciudad simulada.
 *
 * <p>Las coordenadas son sinteticas: una cuadricula de 4 x 3 zonas de ~2 km de lado ubicada en el
 * area metropolitana de Guatemala. Se usan solo para dibujar mapas de calor y resolver la zona de
 * un evento que trae coordenadas pero no nombre (reportes ciudadanos).
 */
public final class ZoneCatalog {

    public static final int COLUMNS = 4;
    public static final int ROWS = 3;
    private static final double ORIGIN_LAT = 14.640;
    private static final double ORIGIN_LON = -90.560;
    private static final double STEP = 0.020;

    private static final List<Zone> ZONES;

    static {
        List<Zone> zones = new ArrayList<>();
        int id = 1;
        for (int row = 0; row < ROWS; row++) {
            for (int col = 0; col < COLUMNS; col++) {
                zones.add(new Zone(id, "Zona " + id, ORIGIN_LAT - row * STEP, ORIGIN_LON + col * STEP, row, col));
                id++;
            }
        }
        ZONES = Collections.unmodifiableList(zones);
    }

    private ZoneCatalog() {
    }

    public static List<Zone> all() {
        return ZONES;
    }

    public static Zone byId(int id) {
        if (id < 1 || id > ZONES.size()) {
            throw new IllegalArgumentException("Zona inexistente: " + id);
        }
        return ZONES.get(id - 1);
    }

    /** Resuelve "Zona 4", "zona 4", "Z4", "4" o "ZONA_4" a la zona correspondiente. */
    public static Optional<Zone> byName(String name) {
        if (name == null || name.isBlank()) {
            return Optional.empty();
        }
        String normalized = Normalizer.normalize(name, Normalizer.Form.NFD)
                .replaceAll("\\p{M}", "")
                .toUpperCase(Locale.ROOT)
                .replaceAll("[^A-Z0-9]", "");
        String digits = normalized.replaceFirst("^(ZONA|Z)", "");
        if (!digits.matches("\\d{1,2}")) {
            return Optional.empty();
        }
        int id = Integer.parseInt(digits);
        return id >= 1 && id <= ZONES.size() ? Optional.of(ZONES.get(id - 1)) : Optional.empty();
    }

    /** Zona cuyo centroide esta mas cerca del punto dado (geocercado simple). */
    public static Zone nearest(double latitude, double longitude) {
        Zone best = ZONES.get(0);
        double bestDistance = Double.MAX_VALUE;
        for (Zone zone : ZONES) {
            double d = zone.distanceKm(latitude, longitude);
            if (d < bestDistance) {
                bestDistance = d;
                best = zone;
            }
        }
        return best;
    }

    /** Zonas adyacentes (arriba, abajo, izquierda, derecha). Usado para simular trayectorias de fuga. */
    public static List<Zone> neighbors(int zoneId) {
        Zone zone = byId(zoneId);
        List<Zone> result = new ArrayList<>();
        int[][] moves = {{-1, 0}, {1, 0}, {0, -1}, {0, 1}};
        for (int[] move : moves) {
            int r = zone.row() + move[0];
            int c = zone.col() + move[1];
            if (r >= 0 && r < ROWS && c >= 0 && c < COLUMNS) {
                result.add(ZONES.get(r * COLUMNS + c));
            }
        }
        return result;
    }
}
