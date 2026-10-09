package gt.umg.centinela.common.privacy;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.HexFormat;
import java.util.Locale;

/**
 * Seudonimizacion de datos personales (placas e identificadores de ciudadanos).
 *
 * <p>Se usa HMAC-SHA256 con una llave secreta en lugar de un hash simple: con SHA-256 "pelado"
 * cualquiera podria calcular el hash de todas las placas posibles (son pocas combinaciones) y
 * revertir el seudonimo. Con HMAC, sin la llave no es posible. La misma placa siempre produce el
 * mismo seudonimo, por lo que el sistema puede correlacionar (seguir un vehiculo entre zonas o
 * compararlo con una lista de vehiculos robados) sin almacenar nunca la placa en claro.
 */
public final class PlatePseudonymizer {

    private static final String ALGORITHM = "HmacSHA256";
    private final SecretKeySpec key;

    public PlatePseudonymizer(String secret) {
        if (secret == null || secret.length() < 16) {
            throw new IllegalArgumentException("La llave HMAC debe tener al menos 16 caracteres");
        }
        this.key = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), ALGORITHM);
    }

    /** Normaliza la placa: mayusculas, sin espacios ni guiones ("p-123 bcd" -> "P123BCD"). */
    public static String normalizePlate(String plate) {
        return plate == null ? null : plate.toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9]", "");
    }

    /** Seudonimo estable de una placa (32 caracteres hexadecimales = 128 bits). */
    public String pseudonymizePlate(String plate) {
        return hmac("PLATE:" + normalizePlate(plate));
    }

    /** Seudonimo estable de un identificador de ciudadano o dispositivo. */
    public String pseudonymizeId(String id) {
        return id == null || id.isBlank() ? null : hmac("ID:" + id);
    }

    /** Placa enmascarada para mostrar a operadores: conserva los 3 primeros caracteres ("XYZ***"). */
    public static String maskPlate(String plate) {
        String normalized = normalizePlate(plate);
        if (normalized == null || normalized.length() <= 3) {
            return "***";
        }
        return normalized.substring(0, 3) + "***";
    }

    private String hmac(String value) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(key);
            byte[] digest = mac.doFinal(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, 16);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("No se pudo calcular HMAC", e);
        }
    }
}
