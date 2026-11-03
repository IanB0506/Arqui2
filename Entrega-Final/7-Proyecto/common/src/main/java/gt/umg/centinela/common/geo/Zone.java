package gt.umg.centinela.common.geo;

public record Zone(int id, String name, double latitude, double longitude, int row, int col) {
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
