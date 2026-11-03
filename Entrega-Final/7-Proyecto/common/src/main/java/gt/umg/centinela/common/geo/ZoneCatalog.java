package gt.umg.centinela.common.geo;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

public final class ZoneCatalog {
    public static final int COLUMNS = 4;
    public static final int ROWS = 3;

    private static final double[][] CENTROIDS = {
            {14.6420, -90.5130},
            {14.6595, -90.5120},
            {14.6360, -90.5250},
            {14.6240, -90.5165},
            {14.6260, -90.4980},
            {14.6530, -90.4960},
            {14.6350, -90.5480},
            {14.6260, -90.5320},
            {14.6060, -90.5180},
            {14.5960, -90.5060},
            {14.6080, -90.5450},
            {14.5900, -90.5380},
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
