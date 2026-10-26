package gt.umg.centinela.simulator.scenario;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.function.IntFunction;

/**
 * Definicion de un escenario reproducible.
 *
 * @param id               identificador usado en la API ("robo-en-curso")
 * @param title            titulo legible
 * @param description      que simula y que se espera
 * @param defaultZone      zona por defecto (cada escenario usa una distinta para no interferir)
 * @param includeInValidation si forma parte de la validacion automatica de correlaciones
 * @param fixedZone        si el escenario ignora la zona pedida (p. ej. la tabla 2.4 usa siempre 4 y 10)
 * @param planner          genera el plan para una zona dada
 */
public record ScenarioDefinition(
        String id,
        String title,
        String description,
        int defaultZone,
        boolean includeInValidation,
        boolean fixedZone,
        @JsonIgnore IntFunction<ScenarioPlan> planner) {
}
