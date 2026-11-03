package gt.umg.centinela.common.model.raw;

import com.fasterxml.jackson.annotation.JsonProperty;

public record UserContext(
        @JsonProperty("usuario_id") String userId,
        @JsonProperty("plataforma") String platform,
        @JsonProperty("version_app") String appVersion) {
}
