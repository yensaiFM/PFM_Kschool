package pfm.pelis.kafka;

import org.apache.kafka.clients.producer.*;

import java.io.*;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;


public class ProducerReview extends Thread {

    public static String KAFKA_HOST = "localhost:9092";
    public static String TOPIC = "review";
    public static final String dirName = "dataset/movies/pending/ratings";
    public static final String dirNameProc = "dataset/movies/processed/ratings";
    private final KafkaProducer<String, Review> producer;
    private final Boolean isAsync = false;

    public ProducerReview(String topic, Boolean isAsync) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "serializers.ReviewSerializer");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "pfm.pelis.kafka.SimplePartitioner");
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        producer = new KafkaProducer<>(props);
    }

    public void sendMessage(String key, Review value) {
        long startTime = System.currentTimeMillis();
        if (isAsync) { // Send asynchronously
            System.out.println("Sent message: (" + key + ", " + value.toString() + ")");
            producer.send(
                    new ProducerRecord<String, Review>(TOPIC, key, value),
                    (Callback) new ReviewCallBack(startTime, key, value));
        } else { // Send synchronously
            try {
                final ProducerRecord<String, Review> record =
                        new ProducerRecord<String, Review>(TOPIC, key, value);
                RecordMetadata metadata = producer.send(record).get();
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
        ProducerReview producer = new ProducerReview(TOPIC, false);
        int lineCount = 0;
        FileInputStream fis;
        BufferedReader br = null;

        try {
            File curFolder = new File(dirName);
            for (File file : curFolder.listFiles()) {
                if (file.isFile()) {
                    System.out.println(" file: " + file.getName());
                    fis = new FileInputStream(dirName + "/" + file.getName());
                    //Construct BufferedReader from InputStreamReader
                    br = new BufferedReader(new InputStreamReader(fis));

                    String line = null;
                    while ((line = br.readLine()) != null) {
                        if(lineCount > 0) {
                            // We must create object Review
                            String[] data_review = line.split(",");
                            int userId = Integer.parseInt(data_review[0]);
                            int movieId = Integer.parseInt(data_review[1]);
                            float rating = Float.parseFloat(data_review[2]);
                            //Long timestamp = Long.parseLong(data_review[3])*1000;
                            // We must convert review to 2017 year. Dataset has older reviews
                            Date date = new Date(Long.parseLong(data_review[3])*1000);
                            Calendar calActual = Calendar.getInstance();
                            calActual.setTimeZone(TimeZone.getDefault());
                            Calendar calReview = Calendar.getInstance();
                            calReview.setTimeZone(TimeZone.getDefault());
                            calReview.setTime(date);
                            int yearReview = calReview.get(Calendar.YEAR);
                            int yearActual = calActual.get(Calendar.YEAR);
                            int addYear = yearActual - yearReview - 1;
                            calReview.add(Calendar.YEAR, addYear);
                            Long timestamp = calReview.getTimeInMillis();
                            Review review = new Review(userId, movieId, rating, timestamp);
                            System.out.println(" review: " + review.toString());
                            // Set key message equal movieId
                            producer.sendMessage(data_review[1], review);
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

class ReviewCallBack implements Callback {

    private long startTime;
    private String key;
    private Review message;

    public ReviewCallBack(long startTime, String key, Review message) {
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
