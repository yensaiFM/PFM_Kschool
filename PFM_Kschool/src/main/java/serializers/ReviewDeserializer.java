package serializers;

import org.apache.kafka.common.serialization.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import pfm.pelis.kafka.Review;
import java.util.Map;

public class ReviewDeserializer implements Deserializer<Review> {

    public void configure(Map configs, boolean isKey) {

    }

    public Review deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        Review review = null;
        if(data != null) {
            try {
                review = mapper.readValue(data, Review.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return review;
    }

    public void close() {

    }
}
