package gt.umg.centinela.simulator.replay;

import gt.umg.centinela.common.model.SensorType;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CsvReplayServiceTest {

    @Test
    void parsesBundledCsv() throws Exception {
        List<CsvReplayService.ReplayRow> rows =
                CsvReplayService.parse(new ClassPathResource("replay/noche-de-viernes.csv").getInputStream());
        assertThat(rows).hasSize(15);
        assertThat(rows.get(5).sensor()).isEqualTo(SensorType.LPR);
        assertThat(rows.get(5).plate()).isEqualTo("P321JKL");
        assertThat(rows.get(5).value()).isEqualTo(95.0);
    }
}
