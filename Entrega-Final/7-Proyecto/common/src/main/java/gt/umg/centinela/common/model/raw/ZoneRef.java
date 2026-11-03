package gt.umg.centinela.common.model.raw;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ZoneRef(
        @JsonProperty("nombre") String name,
        @JsonProperty("latitud") Double latitude,
        @JsonProperty("longitud") Double longitude) {
    public boolean hasCoordinates() {
        return latitude != null && longitude != null;
    }
}
