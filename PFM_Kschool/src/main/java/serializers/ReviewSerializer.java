package serializers;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pfm.pelis.kafka.Review;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

public class ReviewSerializer implements Serializer<Review>{

    public void configure(Map<String, ?> configs, boolean isKey) {
        // Nothing to do
    }

    public byte[] serialize(String topic, Review review) {
        if(review == null){
            return null;
        }

        byte[] reviewVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            reviewVal = objectMapper.writeValueAsString(review).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return reviewVal;
    }

    public void close() {
        // Nothing to do
    }
}
