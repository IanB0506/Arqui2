package gt.umg.centinela.common.model.raw;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Contexto del usuario cuando el boton de panico proviene de la app movil. */
public record UserContext(
        @JsonProperty("usuario_id") String userId,
        @JsonProperty("plataforma") String platform,
        @JsonProperty("version_app") String appVersion) {
}
