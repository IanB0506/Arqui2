package gt.umg.centinela.common.json;

/**
 * Error no recuperable: el mensaje esta mal formado o no cumple el contrato.
 * Reintentar no sirve de nada, por eso va directo a la cola de mensajes muertos (DLQ).
 */
public class InvalidEventException extends RuntimeException {

    public InvalidEventException(String message) {
        super(message);
    }

    public InvalidEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
