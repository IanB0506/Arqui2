package gt.umg.centinela.simulator.scenario;

import gt.umg.centinela.common.geo.Zone;
import gt.umg.centinela.common.geo.ZoneCatalog;
import gt.umg.centinela.common.model.raw.CitizenEventType;
import gt.umg.centinela.common.model.raw.PanicAlertType;
import gt.umg.centinela.common.model.raw.ReportOrigin;
import gt.umg.centinela.common.model.raw.SoundType;
import gt.umg.centinela.common.privacy.PlatePseudonymizer;
import gt.umg.centinela.simulator.config.SimulatorProperties;
import gt.umg.centinela.simulator.generator.BaselineGenerator;
import gt.umg.centinela.simulator.generator.EventFactory;
import gt.umg.centinela.simulator.publish.EventPublisher;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Catalogo de escenarios de los casos de uso del enunciado (seccion 7).
 *
 * <p>Cada escenario usa por defecto una zona distinta para que puedan ejecutarse en paralelo sin
 * que la evidencia de uno contamine al otro:
 * robo 4 (fuga por 8 y 12), falso positivo 10, accidente 7, disparos 2, incendio 11,
 * vehiculo robado 12 (fuga a 11), boton abusivo 1, emergencia medica 5.
 */
@Component
public class ScenarioCatalog {

    /** Tiempo maximo para ver un posible falso positivo: timeout de corroboracion (90 s) + margen. */
    private static final int FALSE_POSITIVE_WAIT = 150;
    /** Placa del ejemplo de la seccion 2.4 del enunciado. */
    public static final String EXAMPLE_PLATE = "XYZ123";

    private final EventFactory factory;
    private final EventPublisher publisher;
    private final BaselineGenerator baseline;
    private final SimulatorProperties properties;
    private final Map<String, ScenarioDefinition> scenarios = new LinkedHashMap<>();

    public ScenarioCatalog(EventFactory factory, EventPublisher publisher, BaselineGenerator baseline,
                           SimulatorProperties properties) {
        this.factory = factory;
        this.publisher = publisher;
        this.baseline = baseline;
        this.properties = properties;
        register();
    }

    public Collection<ScenarioDefinition> all() {
        return scenarios.values();
    }

    public Optional<ScenarioDefinition> find(String id) {
        return Optional.ofNullable(scenarios.get(id));
    }

    private void register() {
        add("robo-en-curso", "Robo en curso con vehiculo en fuga",
                "Boton de panico (ROBO) + exceso de velocidad + camara LPR con la placa XYZ123 a alta velocidad. "
                        + "Luego la misma placa aparece en zonas vecinas. Se espera ROBO_EN_CURSO (escala a CRITICAL) "
                        + "y VEHICULO_EN_FUGA en la siguiente zona.",
                4, true, this::robbery);
        add("falso-positivo", "Boton de panico sin eventos complementarios",
                "Un unico boton de panico sin ninguna otra senal en la zona. Tras el tiempo de corroboracion "
                        + "se espera POSIBLE_FALSO_POSITIVO (fila 2 de la tabla 2.4).",
                10, true, this::falsePositive);
        add("tabla-2-4", "Ejemplo exacto de la tabla 2.4 del enunciado",
                "Fila 1: Zona 4, panico + placa XYZ123 + 85 km/h -> robo en curso. "
                        + "Fila 2: Zona 10, panico sin eventos complementarios -> falso positivo. "
                        + "La zona indicada se ignora.",
                4, false, true, zone -> table24());
        add("accidente-vial", "Accidente vial",
                "Sensor acustico detecta choque, sensor de velocidad registra 97 km/h y un ciudadano reporta "
                        + "el accidente desde la app. Se espera ACCIDENTE_VIAL (primero por sensores, escala con el reporte).",
                7, true, this::trafficAccident);
        add("disparos", "Disparos confirmados",
                "Dos sensores acusticos detectan disparos, un ciudadano presiona panico (ASALTO) y otro reporta un "
                        + "altercado. Se espera DISPAROS_CONFIRMADOS.",
                2, true, this::gunshots);
        add("incendio", "Incendio estructural",
                "Dos reportes ciudadanos de incendio, un boton de panico INCENDIO y una explosion detectada. "
                        + "Se espera INCENDIO_ESTRUCTURAL.",
                11, true, this::fire);
        add("vehiculo-robado", "Vehiculo con reporte de robo",
                "Una camara LPR lee una placa que esta en la lista de vehiculos robados; un minuto despues aparece "
                        + "en una zona vecina. Se espera VEHICULO_REPORTADO.",
                12, true, this::stolenVehicle);
        add("boton-abusivo", "Dispositivo que abusa del boton de panico",
                "La misma app presiona panico 7 veces en 35 segundos. Se espera DISPOSITIVO_SOSPECHOSO y que sus "
                        + "panicos ya no generen alertas (mitigacion de falsos positivos).",
                1, true, this::abusiveDevice);
        add("emergencia-medica", "Emergencia medica",
                "Boton de panico de tipo EMERGENCIA_MEDICA: no requiere corroboracion. Se espera EMERGENCIA_MEDICA inmediata.",
                5, true, this::medicalEmergency);
        add("pico-de-carga", "Pico de carga con robo simultaneo",
                "Multiplica x20 el trafico de fondo durante 60 s y a los 20 s ejecuta un robo en curso. Sirve para "
                        + "medir si el SLO de latencia (< 2 s) se sostiene bajo carga.",
                4, false, this::loadPeak);
    }

    private void add(String id, String title, String description, int zone, boolean validation,
                     java.util.function.IntFunction<ScenarioPlan> planner) {
        add(id, title, description, zone, validation, false, planner);
    }

    private void add(String id, String title, String description, int zone, boolean validation, boolean fixedZone,
                     java.util.function.IntFunction<ScenarioPlan> planner) {
        scenarios.put(id, new ScenarioDefinition(id, title, description, zone, validation, fixedZone, planner));
    }

    // ------------------------------------------------------------------ escenarios

    private ScenarioPlan robbery(int zone) {
        List<Integer> route = fleeRoute(zone, 2);
        String device = factory.randomAppDevice();
        List<ScenarioStep> steps = new ArrayList<>();
        steps.add(step(0, "Boton de panico ROBO desde la app en Zona " + zone,
                () -> publisher.publish(factory.panic(zone, PanicAlertType.ROBO, device))));
        steps.add(step(15_000, "Sensor de velocidad registra 85 km/h en Zona " + zone,
                () -> publisher.publish(factory.speed(zone, 2, 85, false))));
        steps.add(step(18_000, "Camara LPR lee placa " + masked(EXAMPLE_PLATE) + " a 87 km/h en Zona " + zone,
                () -> publisher.publish(factory.lpr(zone, 1, EXAMPLE_PLATE, 87))));
        List<ExpectedAlert> expected = new ArrayList<>();
        expected.add(new ExpectedAlert("ROBO_EN_CURSO", zone, 30, "CRITICAL"));
        long offset = 90_000;
        for (int next : route) {
            double speed = 90 + (offset / 30_000.0);
            steps.add(step(offset, "Placa " + masked(EXAMPLE_PLATE) + " detectada en fuga en Zona " + next,
                    () -> publisher.publish(factory.lpr(next, 2, EXAMPLE_PLATE, speed))));
            offset += 60_000;
        }
        if (!route.isEmpty()) {
            expected.add(new ExpectedAlert("VEHICULO_EN_FUGA", route.get(0), 110, "HIGH"));
        }
        return new ScenarioPlan(steps, expected, withRoute(zone, route));
    }

    private ScenarioPlan falsePositive(int zone) {
        String device = factory.randomAppDevice();
        return new ScenarioPlan(
                List.of(step(0, "Boton de panico PANICO sin ninguna otra senal en Zona " + zone,
                        () -> publisher.publish(factory.panic(zone, PanicAlertType.PANICO, device)))),
                List.of(new ExpectedAlert("POSIBLE_FALSO_POSITIVO", zone, FALSE_POSITIVE_WAIT, "LOW")));
    }

    private ScenarioPlan table24() {
        String device = factory.randomAppDevice();
        String device2 = factory.randomAppDevice();
        return new ScenarioPlan(List.of(
                step(0, "Fila 1: Boton de panico en Zona 4",
                        () -> publisher.publish(factory.panic(4, PanicAlertType.ROBO, device))),
                step(0, "Fila 2: Boton de panico en Zona 10",
                        () -> publisher.publish(factory.panic(10, PanicAlertType.PANICO, device2))),
                step(10_000, "Fila 1: Lectura LPR placa " + masked(EXAMPLE_PLATE) + " en Zona 4",
                        () -> publisher.publish(factory.lpr(4, 1, EXAMPLE_PLATE, 84))),
                step(12_000, "Fila 1: Sensor de velocidad 85 km/h en Zona 4",
                        () -> publisher.publish(factory.speed(4, 2, 85, false)))),
                List.of(new ExpectedAlert("ROBO_EN_CURSO", 4, 30, "CRITICAL"),
                        new ExpectedAlert("POSIBLE_FALSO_POSITIVO", 10, FALSE_POSITIVE_WAIT, "LOW")));
    }

    private ScenarioPlan trafficAccident(int zone) {
        return new ScenarioPlan(List.of(
                step(0, "Sensor acustico detecta CHOQUE_VEHICULAR (112 dB, p=0.88) en Zona " + zone,
                        () -> publisher.publish(factory.acoustic(zone, 1, SoundType.CHOQUE_VEHICULAR, 112, 0.88))),
                step(4_000, "Sensor de velocidad registra 97 km/h en Zona " + zone,
                        () -> publisher.publish(factory.speed(zone, 3, 97, false))),
                step(20_000, "Ciudadano reporta ACCIDENTE desde la app",
                        () -> publisher.publish(factory.citizen(zone, CitizenEventType.ACCIDENTE, factory.randomCitizenId(),
                                ReportOrigin.APP, "Choque fuerte en el cruce, un carro volcado")))),
                List.of(new ExpectedAlert("ACCIDENTE_VIAL", zone, 40, "CRITICAL")));
    }

    private ScenarioPlan gunshots(int zone) {
        return new ScenarioPlan(List.of(
                step(0, "Sensor acustico 1 detecta DISPARO (128 dB, p=0.93)",
                        () -> publisher.publish(factory.acoustic(zone, 1, SoundType.DISPARO, 128, 0.93))),
                step(1_500, "Sensor acustico 2 detecta DISPARO (124 dB, p=0.90)",
                        () -> publisher.publish(factory.acoustic(zone, 2, SoundType.DISPARO, 124, 0.90))),
                step(10_000, "Boton de panico ASALTO en un poste fisico",
                        () -> publisher.publish(factory.panic(zone, PanicAlertType.ASALTO, EventFactory.panicButton(zone, 3)))),
                step(25_000, "Ciudadano reporta ALTERCADO",
                        () -> publisher.publish(factory.citizen(zone, CitizenEventType.ALTERCADO, factory.randomCitizenId(),
                                ReportOrigin.APP, "Se escucharon disparos y gente corriendo")))),
                List.of(new ExpectedAlert("DISPAROS_CONFIRMADOS", zone, 30, "CRITICAL")));
    }

    private ScenarioPlan fire(int zone) {
        return new ScenarioPlan(List.of(
                step(0, "Ciudadano reporta INCENDIO",
                        () -> publisher.publish(factory.citizen(zone, CitizenEventType.INCENDIO, factory.randomCitizenId(),
                                ReportOrigin.APP, "Sale humo y fuego de una bodega"))),
                step(8_000, "Boton de panico INCENDIO en poste fisico",
                        () -> publisher.publish(factory.panic(zone, PanicAlertType.INCENDIO, EventFactory.panicButton(zone, 1)))),
                step(15_000, "Sensor acustico detecta EXPLOSION (118 dB, p=0.81)",
                        () -> publisher.publish(factory.acoustic(zone, 1, SoundType.EXPLOSION, 118, 0.81))),
                step(30_000, "Segundo ciudadano reporta INCENDIO desde un punto fisico",
                        () -> publisher.publish(factory.citizen(zone, CitizenEventType.INCENDIO, factory.randomCitizenId(),
                                ReportOrigin.PUNTO_FISICO, "Incendio en bodega, llamen a bomberos")))),
                List.of(new ExpectedAlert("INCENDIO_ESTRUCTURAL", zone, 30, "CRITICAL")));
    }

    private ScenarioPlan stolenVehicle(int zone) {
        String plate = properties.getWatchlistPlates().isEmpty() ? "P512KLM" : properties.getWatchlistPlates().get(0);
        List<Integer> route = fleeRoute(zone, 1);
        List<ScenarioStep> steps = new ArrayList<>();
        steps.add(step(0, "Camara LPR lee placa en lista de robados " + masked(plate) + " (55 km/h)",
                () -> publisher.publish(factory.lpr(zone, 3, plate, 55))));
        for (int next : route) {
            steps.add(step(60_000, "La misma placa aparece en Zona " + next,
                    () -> publisher.publish(factory.lpr(next, 1, plate, 68))));
        }
        return new ScenarioPlan(steps, List.of(new ExpectedAlert("VEHICULO_REPORTADO", zone, 20, "HIGH")),
                withRoute(zone, route));
    }

    private ScenarioPlan abusiveDevice(int zone) {
        String device = "APP-ABUSO" + (System.currentTimeMillis() % 1000);
        List<ScenarioStep> steps = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            int press = i + 1;
            steps.add(step(i * 5_000L, "Pulsacion " + press + " del mismo dispositivo " + device,
                    () -> publisher.publish(factory.panic(zone, PanicAlertType.PANICO, device))));
        }
        return new ScenarioPlan(steps, List.of(new ExpectedAlert("DISPOSITIVO_SOSPECHOSO", zone, 60, "LOW")));
    }

    private ScenarioPlan medicalEmergency(int zone) {
        String device = factory.randomAppDevice();
        return new ScenarioPlan(
                List.of(step(0, "Boton de panico EMERGENCIA_MEDICA",
                        () -> publisher.publish(factory.panic(zone, PanicAlertType.EMERGENCIA_MEDICA, device)))),
                List.of(new ExpectedAlert("EMERGENCIA_MEDICA", zone, 10, "HIGH")));
    }

    private ScenarioPlan loadPeak(int zone) {
        List<ScenarioStep> steps = new ArrayList<>();
        steps.add(step(0, "Inicio de pico de carga x20 durante 60 s", () -> baseline.startBurst(20, 60)));
        ScenarioPlan robbery = robbery(zone);
        for (ScenarioStep s : robbery.steps()) {
            steps.add(step(20_000 + s.offsetMillis(), "[bajo carga] " + s.description(), s.action()));
        }
        return new ScenarioPlan(steps, List.of(new ExpectedAlert("ROBO_EN_CURSO", zone, 60, "CRITICAL")), robbery.zones());
    }

    // ------------------------------------------------------------------ utilidades

    /** Los pasos van al log y a la API: la placa se muestra enmascarada, como al operador (ADR-007). */
    private static String masked(String plate) {
        return PlatePseudonymizer.maskPlate(plate);
    }

    private static ScenarioStep step(long offset, String description, Runnable action) {
        return new ScenarioStep(offset, description, action);
    }

    private static List<Integer> withRoute(int zone, List<Integer> route) {
        List<Integer> zones = new ArrayList<>(List.of(zone));
        zones.addAll(route);
        return zones;
    }

    /** Ruta de huida determinista: siempre "hacia el sur" (fila mayor), sin repetir zonas. */
    static List<Integer> fleeRoute(int start, int steps) {
        List<Integer> route = new ArrayList<>();
        Set<Integer> visited = new HashSet<>(List.of(start));
        int current = start;
        for (int i = 0; i < steps; i++) {
            Optional<Zone> next = ZoneCatalog.neighbors(current).stream()
                    .filter(z -> !visited.contains(z.id()))
                    .max(Comparator.comparingInt(Zone::row).thenComparing(Comparator.comparingInt(Zone::col).reversed()));
            if (next.isEmpty()) {
                break;
            }
            current = next.get().id();
            visited.add(current);
            route.add(current);
        }
        return route;
    }
}
