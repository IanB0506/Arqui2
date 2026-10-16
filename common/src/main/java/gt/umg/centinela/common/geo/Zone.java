package gt.umg.centinela.common.geo;

/**
 * Zona de la ciudad simulada.
 *
 * @param id        numero de zona (1..12)
 * @param name      nombre legible ("Zona 4")
 * @param latitude  latitud del centroide
 * @param longitude longitud del centroide
 * @param row       fila en la cuadricula (para calcular zonas vecinas)
 * @param col       columna en la cuadricula
 */
public record Zone(int id, String name, double latitude, double longitude, int row, int col) {

    /** Distancia aproximada en km entre el centroide de la zona y un punto (formula de haversine). */
    public double distanceKm(double lat, double lon) {
        double r = 6371.0;
        double dLat = Math.toRadians(lat - latitude);
        double dLon = Math.toRadians(lon - longitude);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(latitude)) * Math.cos(Math.toRadians(lat))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return 2 * r * Math.asin(Math.sqrt(a));
    }
}
