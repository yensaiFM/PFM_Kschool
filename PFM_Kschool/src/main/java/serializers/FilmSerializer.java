package serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import pfm.pelis.kafka.Film;

import java.util.Map;

public class FilmSerializer implements Serializer<Film>{

    public void configure(Map<String, ?> configs, boolean isKey) {
        // Nothing to do
    }

    public byte[] serialize(String topic, Film film) {
        if(film == null){
            return null;
        }

        byte[] filmVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            filmVal = objectMapper.writeValueAsString(film).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return filmVal;
        /*
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(review);
            byte[] b = baos.toByteArray();
            return b;
        }catch (IOException | RuntimeException e) {
            throw new SerializationException("Error serializing value", e);
        }*/
    }

    public void close() {
        // Nothing to do
    }
}
