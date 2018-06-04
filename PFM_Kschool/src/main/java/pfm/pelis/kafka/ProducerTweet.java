package pfm.pelis.kafka;

import org.apache.kafka.clients.producer.*;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.script.ScriptException;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;


public class ProducerTweet extends Thread {

    public static String KAFKA_HOST = "localhost:9092";
    public static String TOPIC = "tweet";
    public static final String dirName = "dataset/twitter/pending";
    public static final String dirNameFilter = "dataset/twitter/filter";
    public static final String dirNameProc = "dataset/twitter/processed";
    private final KafkaProducer<String, Tweet> producer;
    private final Boolean isAsync = false;

    public ProducerTweet(String topic, Boolean isAsync) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "serializers.TweetSerializer");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "pfm.pelis.kafka.SimplePartitioner");
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        producer = new KafkaProducer<>(props);
    }

    public void sendMessage(String key, Tweet value) {
        long startTime = System.currentTimeMillis();
        if (isAsync) { // Send asynchronously
            System.out.println("Sent message: (" + key + ", " + value.toString() + ")");
            producer.send(
                    new ProducerRecord<String, Tweet>(TOPIC, key, value),
                    (Callback) new TweetCallBack(startTime, key, value));
        } else { // Send synchronously
            try {
                final ProducerRecord<String, Tweet> tweet =
                        new ProducerRecord<String, Tweet>(TOPIC, key, value);
                RecordMetadata metadata = producer.send(tweet).get();
                long elapsedTime = System.currentTimeMillis() - startTime;
                System.out.printf("Sent message (key=%s value=%s) " +
                "meta(partition=%d, offset=%d) time=%d\n", key, value.toString(), metadata.partition(),
                        metadata.offset(), elapsedTime);
                System.out.println("Sent message: (" + key + ", " + value.toString() + ")");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException, FileNotFoundException, IOException {
        ProducerTweet producer = new ProducerTweet(TOPIC, false);
        int lineCount = 0;
        FileInputStream fis;
        //FileInputStream filter;
        BufferedReader br = null;

        try {
            File curFolder = new File(dirName);
            for (File file : curFolder.listFiles()) {
                if (file.isFile()) {
                    System.out.println(" file: " + file.getName());

                    // Obtain name file to obtain filters by day
                    String file_tweets = file.getName();
                    int pos = file_tweets.lastIndexOf(".");
                    if(pos > 0){
                        file_tweets = file_tweets.substring(0, pos);
                    }
                    System.out.println(" file no ext: " + file_tweets);
                    ArrayList<String> filterHastag = new ArrayList<String>();
                    Scanner filter = new Scanner(new File(dirNameFilter + "/" + file_tweets + ".txt"));
                    while(filter.hasNext()){
                        filterHastag.add(filter.next());
                    }
                    filter.close();
                    System.out.println(" arraylist: " + Arrays.toString(filterHastag.toArray()));

                    fis = new FileInputStream(dirName + "/" + file.getName());
                    //Construct BufferedReader from InputStreamReader
                    br = new BufferedReader(new InputStreamReader(fis));

                    String line = null;
                    while ((line = br.readLine()) != null) {
                        if(lineCount > 0) {
                            // We must create objectTweet
                            System.out.println(" line: " + line);
                            try {
                                JSONObject obj = new JSONObject(line);
                                if (obj.has("id")) {
                                    String id = obj.get("id").toString();
                                    String text = obj.get("text").toString();
                                    String user = null;
                                    JSONObject user_info = new JSONObject(obj.get("user").toString());
                                    user = user_info.get("screen_name").toString();
                                    Long timestamp = Long.parseLong(obj.get("timestamp_ms").toString());
                                    String lang = obj.get("lang").toString();
                                    // Debemos obtener el hastag utilizado en text
                                    String hastag = "undefined";
                                    for (String str : filterHastag) {
                                        if (text.contains(str)) {
                                            hastag = str;
                                        }
                                    }
                                    Tweet tweet = new Tweet(id, text, user, timestamp, lang, hastag);
                                    System.out.println(" tweet: " + tweet.toString());
                                    // Set key message equal hastag
                                    producer.sendMessage(hastag, tweet);
                                }
                            }catch(org.json.JSONException exception){
                                // JSONException
                                System.out.println("Exception JSON");
                                exception.printStackTrace();
                            }catch (Exception e) {
                                // TODO Auto-generated catch block
                                System.out.println("Exception line JSONObject");
                                e.printStackTrace();
                            }finally{
                            }
                        }
                        lineCount++;
                    }

                    //We must move file to processed
                    file.renameTo(new File(dirNameProc + "/" + file.getName()));
                }
            }
            //System.exit(1);
        }catch(NullPointerException e){
            e.printStackTrace();
        }catch (Exception e) {
            // TODO Auto-generated catch block
            System.out.println("Unexcepted Exception");
            e.printStackTrace();
        }finally{
        }
    }
}

class TweetCallBack implements Callback {

    private long startTime;
    private String key;
    private Tweet message;

    public TweetCallBack(long startTime, String key, Tweet message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling
     * of request completion. This method will be called when the record sent to
     * the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata
     *            The metadata for the record that was sent (i.e. the partition
     *            and offset). Null if an error occurred.
     * @param exception
     *            The exception thrown during processing of this record. Null if
     *            no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println("message(" + key + ", " + message.toString()
                    + ") sent to partition(" + metadata.partition() + "), "
                    + "offset(" + metadata.offset() + ") in " + elapsedTime
                    + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}
