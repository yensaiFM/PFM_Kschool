package serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import pfm.pelis.kafka.Film;

import java.util.Map;

public class FilmDeserializer implements Deserializer<Film> {

    public void configure(Map configs, boolean isKey) {

    }

    public Film deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        Film film = null;
        if(data != null) {
            try {
                film = mapper.readValue(data, Film.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return film;
    }

    public void close() {

    }
}
