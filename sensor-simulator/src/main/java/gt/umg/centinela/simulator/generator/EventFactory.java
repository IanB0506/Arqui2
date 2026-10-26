package gt.umg.centinela.simulator.generator;

import gt.umg.centinela.common.geo.Zone;
import gt.umg.centinela.common.geo.ZoneCatalog;
import gt.umg.centinela.common.model.SensorType;
import gt.umg.centinela.common.model.raw.AcousticSensorEvent;
import gt.umg.centinela.common.model.raw.CitizenEventType;
import gt.umg.centinela.common.model.raw.CitizenReportEvent;
import gt.umg.centinela.common.model.raw.Direction;
import gt.umg.centinela.common.model.raw.LprCameraEvent;
import gt.umg.centinela.common.model.raw.PanicAlertType;
import gt.umg.centinela.common.model.raw.PanicButtonEvent;
import gt.umg.centinela.common.model.raw.ReportOrigin;
import gt.umg.centinela.common.model.raw.SensorLocation;
import gt.umg.centinela.common.model.raw.SoundType;
import gt.umg.centinela.common.model.raw.SpeedSensorEvent;
import gt.umg.centinela.common.model.raw.UserContext;
import gt.umg.centinela.common.model.raw.ZoneRef;
import gt.umg.centinela.simulator.config.SimulatorProperties;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Fabrica de eventos simulados con distribuciones semirrealistas.
 *
 * <p>Inventario de sensores por zona: 3 camaras LPR, 4 sensores de velocidad (el 4 esta en un
 * bulevar con limite de 80 km/h, el resto 60 km/h), 2 sensores acusticos y 5 botones fisicos.
 * Los botones de la app movil se identifican como APP-xxxx.
 */
@Component
public class EventFactory {

    private static final String CONSONANTS = "BCDFGHJKLMNPQRSTVWXYZ";
    private static final List<String> MODELS = List.of(
            "Toyota Hilux", "Toyota Corolla", "Nissan Sentra", "Honda Civic", "Mazda 3", "Kia Rio",
            "Hyundai Tucson", "Mitsubishi L200", "Suzuki Swift", "Isuzu D-Max", "Yamaha FZ", "Honda CB190");
    private static final List<String> COLORS = List.of(
            "Blanco", "Negro", "Gris", "Plata", "Rojo", "Azul", "Verde", "Beige");

    private final SimulatorProperties properties;

    public EventFactory(SimulatorProperties properties) {
        this.properties = properties;
    }

    // ------------------------------------------------------------------ botones de panico

    public SimulatedEvent panic(int zoneId, PanicAlertType type, String deviceId) {
        Zone zone = ZoneCatalog.byId(zoneId);
        double[] point = randomPointIn(zone, 0.006);
        UserContext context = deviceId.startsWith("APP-")
                ? new UserContext("CIU-" + deviceId.substring(4), random().nextBoolean() ? "ANDROID" : "IOS", "2.3.1")
                : null;
        UUID id = UUID.randomUUID();
        PanicButtonEvent event = new PanicButtonEvent(id, Instant.now(),
                new ZoneRef(zone.name(), point[0], point[1]), type, deviceId, context);
        return new SimulatedEvent(SensorType.PANIC, deviceId, id, event);
    }

    public SimulatedEvent randomPanic(int zoneId) {
        PanicAlertType type = weighted(
                new PanicAlertType[]{PanicAlertType.PANICO, PanicAlertType.ROBO, PanicAlertType.ASALTO,
                        PanicAlertType.EMERGENCIA_MEDICA, PanicAlertType.INCENDIO, PanicAlertType.ACCIDENTE},
                new double[]{30, 25, 20, 15, 5, 5});
        String device = random().nextDouble() < 0.6 ? randomAppDevice() : panicButton(zoneId, random().nextInt(1, 6));
        return panic(zoneId, type, device);
    }

    // ------------------------------------------------------------------ camaras LPR

    public SimulatedEvent lpr(int zoneId, int cameraIndex, String plate, double speed) {
        Zone zone = ZoneCatalog.byId(zoneId);
        String sensorId = sensorId("LPR", zoneId, cameraIndex);
        double[] point = fixedSensorPoint(zone, sensorId);
        UUID id = UUID.randomUUID();
        LprCameraEvent event = new LprCameraEvent(id, Instant.now(), plate,
                new SensorLocation(sensorId, zone.name(), point[0], point[1]),
                round1(speed), pick(MODELS), pick(COLORS));
        return new SimulatedEvent(SensorType.LPR, sensorId, id, event);
    }

    public SimulatedEvent randomLpr(int zoneId) {
        if (random().nextDouble() >= properties.getLprAnomalyProbability()) {
            return normalLpr(zoneId);
        }
        return lpr(zoneId, random().nextInt(1, 4), randomPlate(), random().nextDouble(90, 130));
    }

    /** Lectura sin novedad: velocidad normal (nunca llega al umbral de exceso). */
    public SimulatedEvent normalLpr(int zoneId) {
        return lpr(zoneId, random().nextInt(1, 4), randomPlate(), clamp(random().nextGaussian(45, 12), 5, 75));
    }

    // ------------------------------------------------------------------ sensores de velocidad

    public SimulatedEvent speed(int zoneId, int sensorIndex, double speed, boolean wrongWay) {
        String sensorId = sensorId("VEL", zoneId, sensorIndex);
        UUID id = UUID.randomUUID();
        SpeedSensorEvent event = new SpeedSensorEvent(id, Instant.now(), sensorId, ZoneCatalog.byId(zoneId).name(),
                round1(speed), pick(List.of(Direction.values())), wrongWay, speedLimit(sensorIndex));
        return new SimulatedEvent(SensorType.SPEED, sensorId, id, event);
    }

    public SimulatedEvent randomSpeed(int zoneId) {
        boolean anomaly = random().nextDouble() < properties.getSpeedAnomalyProbability();
        boolean wrongWay = random().nextDouble() < 0.0005;
        if (!anomaly && !wrongWay) {
            return normalSpeed(zoneId);
        }
        int sensorIndex = random().nextInt(1, 5);
        int limit = speedLimit(sensorIndex);
        double speed = anomaly ? limit * random().nextDouble(1.4, 2.0) : normalSpeedFor(limit);
        return speed(zoneId, sensorIndex, speed, wrongWay);
    }

    /** Lectura dentro del limite (hasta 125 %, la ingesta marca exceso desde 130 %), en el sentido correcto. */
    public SimulatedEvent normalSpeed(int zoneId) {
        int sensorIndex = random().nextInt(1, 5);
        return speed(zoneId, sensorIndex, normalSpeedFor(speedLimit(sensorIndex)), false);
    }

    private static double normalSpeedFor(int limit) {
        return clamp(random().nextGaussian(limit * 0.8, limit * 0.15), 3, limit * 1.25);
    }

    // ------------------------------------------------------------------ sensores acusticos

    public SimulatedEvent acoustic(int zoneId, int sensorIndex, SoundType type, double decibels, double probability) {
        String sensorId = sensorId("ACU", zoneId, sensorIndex);
        UUID id = UUID.randomUUID();
        AcousticSensorEvent event = new AcousticSensorEvent(id, Instant.now(), sensorId,
                ZoneCatalog.byId(zoneId).name(), type, round1(decibels), round2(probability));
        return new SimulatedEvent(SensorType.ACOUSTIC, sensorId, id, event);
    }

    public SimulatedEvent randomAcoustic(int zoneId) {
        // La mayoria es ruido ambiental. Los sonidos "criticos" de fondo tienen probabilidad moderada:
        // en Guatemala los cohetes y bombas de feria generan falsos disparos/explosiones con frecuencia.
        SoundType type = weighted(
                new SoundType[]{SoundType.RUIDO_AMBIENTAL, SoundType.VIDRIOS_ROTOS, SoundType.CHOQUE_VEHICULAR,
                        SoundType.EXPLOSION, SoundType.DISPARO},
                new double[]{95, 2, 1.5, 1, 0.5});
        if (type == SoundType.RUIDO_AMBIENTAL) {
            return ambientAcoustic(zoneId);
        }
        return acoustic(zoneId, random().nextInt(1, 3), type, random().nextDouble(90, 125), random().nextDouble(0.25, 0.72));
    }

    /** Ruido ambiental: nunca es un sonido critico. */
    public SimulatedEvent ambientAcoustic(int zoneId) {
        return acoustic(zoneId, random().nextInt(1, 3), SoundType.RUIDO_AMBIENTAL, random().nextDouble(55, 85),
                random().nextDouble(0.01, 0.2));
    }

    // ------------------------------------------------------------------ reportes ciudadanos

    public SimulatedEvent citizen(int zoneId, CitizenEventType type, String reporterId, ReportOrigin origin, String message) {
        double[] point = randomPointIn(ZoneCatalog.byId(zoneId), 0.007);
        UUID id = UUID.randomUUID();
        // El reporte trae solo coordenadas aproximadas (sin nombre de zona): la ingesta resuelve la zona.
        CitizenReportEvent event = new CitizenReportEvent(id, Instant.now(), type, message,
                new ZoneRef(null, point[0], point[1]), origin, reporterId);
        return new SimulatedEvent(SensorType.CITIZEN_REPORT, reporterId, id, event);
    }

    public SimulatedEvent randomCitizen(int zoneId) {
        CitizenEventType type = weighted(
                new CitizenEventType[]{CitizenEventType.OTRO, CitizenEventType.PERSONA_SOSPECHOSA,
                        CitizenEventType.ALTERCADO, CitizenEventType.ACCIDENTE, CitizenEventType.ROBO,
                        CitizenEventType.INCENDIO},
                new double[]{35, 20, 15, 15, 10, 5});
        ReportOrigin origin = weighted(new ReportOrigin[]{ReportOrigin.APP, ReportOrigin.PUNTO_FISICO, ReportOrigin.USUARIO},
                new double[]{70, 20, 10});
        return citizen(zoneId, type, randomCitizenId(), origin, messageFor(type));
    }

    // ------------------------------------------------------------------ utilidades

    public static String sensorId(String prefix, int zoneId, int index) {
        return String.format(Locale.ROOT, "%s-Z%02d-%02d", prefix, zoneId, index);
    }

    public static String panicButton(int zoneId, int index) {
        return sensorId("BTN", zoneId, index);
    }

    public static int speedLimit(int sensorIndex) {
        return sensorIndex == 4 ? 80 : 60;
    }

    public String randomAppDevice() {
        return String.format(Locale.ROOT, "APP-%05d", random().nextInt(100000));
    }

    public String randomCitizenId() {
        return String.format(Locale.ROOT, "CIU-%05d", random().nextInt(100000));
    }

    /** Placa con formato guatemalteco: tipo (P particular, M moto, C comercial, A alquiler) + 3 digitos + 3 letras. */
    public String randomPlate() {
        String prefix = weighted(new String[]{"P", "M", "C", "A"}, new double[]{80, 10, 5, 5});
        StringBuilder sb = new StringBuilder(prefix).append(String.format(Locale.ROOT, "%03d", random().nextInt(1000)));
        for (int i = 0; i < 3; i++) {
            sb.append(CONSONANTS.charAt(random().nextInt(CONSONANTS.length())));
        }
        return sb.toString();
    }

    private static String messageFor(CitizenEventType type) {
        return switch (type) {
            case ACCIDENTE -> "Choque entre dos vehiculos, hay personas heridas";
            case INCENDIO -> "Sale humo negro de un local comercial";
            case ALTERCADO -> "Pelea en la via publica, varias personas involucradas";
            case ROBO -> "Acaban de asaltar a una persona en la parada de bus";
            case PERSONA_SOSPECHOSA -> "Persona merodeando vehiculos estacionados";
            case OTRO -> "Semaforo descompuesto en la esquina";
        };
    }

    /**
     * Punto al azar cerca del centroide que sigue perteneciendo a la zona: el geocercado (centroide mas
     * cercano) lo resuelve a esa misma zona, asi un reporte ciudadano nunca cae en la zona vecina.
     */
    private static double[] randomPointIn(Zone zone, double spread) {
        for (int i = 0; i < 20; i++) {
            double lat = zone.latitude() + random().nextDouble(-spread, spread);
            double lon = zone.longitude() + random().nextDouble(-spread, spread);
            if (ZoneCatalog.nearest(lat, lon).id() == zone.id()) {
                return new double[]{lat, lon};
            }
        }
        return new double[]{zone.latitude(), zone.longitude()};
    }

    /** Las camaras son fijas: su posicion se deriva del id para que siempre sea la misma (y dentro de su zona). */
    private static double[] fixedSensorPoint(Zone zone, String sensorId) {
        int h = Math.abs(sensorId.hashCode());
        double dLat = ((h % 100) - 50) / 10000.0;
        double dLon = (((h / 100) % 100) - 50) / 10000.0;
        for (int i = 0; i < 4; i++, dLat /= 2, dLon /= 2) {
            if (ZoneCatalog.nearest(zone.latitude() + dLat, zone.longitude() + dLon).id() == zone.id()) {
                break;
            }
        }
        return new double[]{zone.latitude() + dLat, zone.longitude() + dLon};
    }

    private static <T> T weighted(T[] options, double[] weights) {
        double total = 0;
        for (double w : weights) {
            total += w;
        }
        double r = random().nextDouble(total);
        for (int i = 0; i < options.length; i++) {
            r -= weights[i];
            if (r < 0) {
                return options[i];
            }
        }
        return options[options.length - 1];
    }

    private static <T> T pick(List<T> list) {
        return list.get(random().nextInt(list.size()));
    }

    private static double clamp(double value, double min, double max) {
        return Math.max(min, Math.min(max, value));
    }

    private static double round1(double v) {
        return Math.round(v * 10.0) / 10.0;
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    private static ThreadLocalRandom random() {
        return ThreadLocalRandom.current();
    }
}
