package serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import pfm.pelis.kafka.Tweet;

import java.util.Map;

public class TweetSerializer implements Serializer<Tweet>{

    public void configure(Map<String, ?> configs, boolean isKey) {
        // Nothing to do
    }

    public byte[] serialize(String topic, Tweet tweet) {
        if(tweet == null){
            return null;
        }

        byte[] tweetVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            tweetVal = objectMapper.writeValueAsString(tweet).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tweetVal;
    }

    public void close() {
        // Nothing to do
    }
}
