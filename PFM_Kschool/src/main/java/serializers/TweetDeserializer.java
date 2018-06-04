package serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import pfm.pelis.kafka.Tweet;

import java.util.Map;

public class TweetDeserializer implements Deserializer<Tweet> {

    public void configure(Map configs, boolean isKey) {

    }

    public Tweet deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        Tweet tweet = null;
        if(data != null) {
            try {
                tweet = mapper.readValue(data, Tweet.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return tweet;
    }

    public void close() {

    }
}
