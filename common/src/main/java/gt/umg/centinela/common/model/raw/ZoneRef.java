package gt.umg.centinela.common.model.raw;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Referencia geografica enviada por un sensor: puede traer el nombre de la zona, coordenadas o ambos
 * ("zona_geografica (coordenadas o nombre de la zona)" en el enunciado).
 */
public record ZoneRef(
        @JsonProperty("nombre") String name,
        @JsonProperty("latitud") Double latitude,
        @JsonProperty("longitud") Double longitude) {

    public boolean hasCoordinates() {
        return latitude != null && longitude != null;
    }
}
