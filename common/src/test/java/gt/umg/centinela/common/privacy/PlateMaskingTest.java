package gt.umg.centinela.common.privacy;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Un ciudadano puede escribir una placa en su reporte: no debe quedar en claro en ningun lado. */
class PlateMaskingTest {

    @Test
    void enmascaraPlacasEscritasEnTextoLibre() {
        assertThat(PlatePseudonymizer.maskPlatesInText("Se fue un pickup placa P-512KLM hacia el sur"))
                .isEqualTo("Se fue un pickup placa P51*** hacia el sur");
        assertThat(PlatePseudonymizer.maskPlatesInText("moto m 045 bbc sin luces")).isEqualTo("moto M04*** sin luces");
        assertThat(PlatePseudonymizer.maskPlatesInText("el carro XYZ123 y otro XYZ-124")).isEqualTo("el carro XYZ*** y otro XYZ***");
    }

    @Test
    void noTocaElRestoDelMensaje() {
        String message = "Choque fuerte en el cruce de la 6a avenida y 12 calle, a las 100 metros hay heridos";
        assertThat(PlatePseudonymizer.maskPlatesInText(message)).isEqualTo(message);
        assertThat(PlatePseudonymizer.maskPlatesInText(null)).isNull();
    }
}
