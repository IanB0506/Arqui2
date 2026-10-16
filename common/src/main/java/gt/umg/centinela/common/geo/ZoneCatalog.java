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
 * <p>Cada zona usa el centroide aproximado de la zona real con el mismo numero en la Ciudad de
 * Guatemala (Zona 1 = Centro Historico, Zona 10 = Zona Viva, etc.), para que el mapa de Grafana las
 * dibuje donde corresponde. Las coordenadas se usan para el mapa de calor y para resolver la zona de
 * un evento que trae coordenadas pero no nombre (reportes ciudadanos: zona con el centroide mas cercano).
 *
 * <p>La vecindad para simular rutas de huida usa una cuadricula logica de 4 x 3 (fila, columna); con
 * ella el robo de la tabla 2.4 huye 4 -> 8 -> 12, una ruta plausible tambien en el mapa real.
 */
public final class ZoneCatalog {

    public static final int COLUMNS = 4;
    public static final int ROWS = 3;

    /** Centroides aproximados (latitud, longitud) de las zonas 1 a 12 de la Ciudad de Guatemala. */
    private static final double[][] CENTROIDS = {
            {14.6420, -90.5130},  // Zona 1: Centro Historico
            {14.6595, -90.5120},  // Zona 2: Mapa en Relieve, Hipodromo del Norte
            {14.6360, -90.5250},  // Zona 3: Cementerio General
            {14.6240, -90.5165},  // Zona 4: Centro Civico, 4 Grados Norte
            {14.6260, -90.4980},  // Zona 5: Estadio Mateo Flores
            {14.6530, -90.4960},  // Zona 6
            {14.6350, -90.5480},  // Zona 7: Kaminal Juyu
            {14.6260, -90.5320},  // Zona 8
            {14.6060, -90.5180},  // Zona 9: Torre del Reformador
            {14.5960, -90.5060},  // Zona 10: Zona Viva
            {14.6080, -90.5450},  // Zona 11: Calzada Roosevelt, Miraflores
            {14.5900, -90.5380},  // Zona 12: Ciudad Universitaria (USAC)
    };

    private static final List<Zone> ZONES;

    static {
        List<Zone> zones = new ArrayList<>();
        for (int i = 0; i < CENTROIDS.length; i++) {
            int id = i + 1;
            zones.add(new Zone(id, "Zona " + id, CENTROIDS[i][0], CENTROIDS[i][1], i / COLUMNS, i % COLUMNS));
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
