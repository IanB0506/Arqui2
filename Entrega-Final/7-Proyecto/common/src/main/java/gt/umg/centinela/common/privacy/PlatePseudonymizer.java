package gt.umg.centinela.common.privacy;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.HexFormat;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class PlatePseudonymizer {
    // HMAC con llave secreta: la misma placa da el mismo seudónimo y no se puede revertir sin la llave (ADR-007).
    private static final String ALGORITHM = "HmacSHA256";
    private static final Pattern PLATE_IN_TEXT = Pattern.compile(
            "\\b(?:[PMCAOUT][-\\s]?\\d{3}[-\\s]?[A-Z]{3}|[A-Z]{3}-?\\d{3})\\b", Pattern.CASE_INSENSITIVE);
    private final SecretKeySpec key;

    public PlatePseudonymizer(String secret) {
        if (secret == null || secret.length() < 16) {
            throw new IllegalArgumentException("La llave HMAC debe tener al menos 16 caracteres");
        }
        this.key = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), ALGORITHM);
    }

    public static String normalizePlate(String plate) {
        return plate == null ? null : plate.toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9]", "");
    }

    public String pseudonymizePlate(String plate) {
        return hmac("PLATE:" + normalizePlate(plate));
    }

    public String pseudonymizeId(String id) {
        return id == null || id.isBlank() ? null : hmac("ID:" + id);
    }

    public static String maskPlatesInText(String text) {
        return text == null ? null
                : PLATE_IN_TEXT.matcher(text).replaceAll(m -> Matcher.quoteReplacement(maskPlate(m.group())));
    }

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
