package gt.umg.centinela.common.spool;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpoolLineCodecTest {

    @Test
    void idaYVueltaDeLosTresTiposDeRegistro() {
        SpoolEntry e = new SpoolEntry("id-1", "sensor.panic.raw", "dev-9", 1234L, 2, "{\"a\":\"x\\ty\"}");
        assertThat(SpoolLineCodec.parse(SpoolLineCodec.put(e))).isEqualTo(new SpoolLineCodec.Put(e));
        assertThat(SpoolLineCodec.parse(SpoolLineCodec.ack("id-1"))).isEqualTo(new SpoolLineCodec.Ack("id-1"));
        assertThat(SpoolLineCodec.parse(SpoolLineCodec.fail("id-1"))).isEqualTo(new SpoolLineCodec.Fail("id-1"));
    }

    @Test
    void llaveNulaSeConserva() {
        SpoolEntry e = new SpoolEntry("id-2", "t", null, 1L, 0, "{}");
        SpoolLineCodec.Parsed p = SpoolLineCodec.parse(SpoolLineCodec.put(e));
        assertThat(((SpoolLineCodec.Put) p).entry().key()).isNull();
    }

    @Test
    void payloadConTabuladorLiteralSobreviveYCrcDetectaCorrupcion() {
        SpoolEntry e = new SpoolEntry("id-3", "t", "k", 1L, 0, "{\"x\":1}\tcola");
        String line = SpoolLineCodec.put(e);
        assertThat(SpoolLineCodec.parse(line)).isEqualTo(new SpoolLineCodec.Put(e));
        String corrupt = line.replace("cola", "colb");
        assertThat(SpoolLineCodec.parse(corrupt)).isInstanceOf(SpoolLineCodec.Invalid.class);
        assertThat(SpoolLineCodec.parse(line.substring(0, line.length() - 3))).isInstanceOf(SpoolLineCodec.Invalid.class);
    }

    @Test
    void saltosDeLineaSeRechazanAlCrearLaEntrada() {
        assertThatThrownBy(() -> new SpoolEntry("id", "t", "k", 1L, 0, "{\n}"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SpoolEntry("id", "t", "k\nx", 1L, 0, "{}"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
