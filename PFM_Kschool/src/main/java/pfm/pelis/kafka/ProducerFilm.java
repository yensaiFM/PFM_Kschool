package pfm.pelis.kafka;

import org.apache.kafka.clients.producer.*;

import java.io.*;
import org.json.*;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;


public class ProducerFilm extends Thread {

    public static String KAFKA_HOST = "localhost:9092";
    public static String TOPIC = "film";
    public static final String dirName = "dataset/movies/pending/films";
    public static final String dirNameProc = "dataset/movies/processed/films";
    private final KafkaProducer<String, Film> producer;
    private final Boolean isAsync = false;

    public ProducerFilm(String topic, Boolean isAsync) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "serializers.FilmSerializer");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "pfm.pelis.kafka.SimplePartitioner");
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        producer = new KafkaProducer<>(props);
    }

    public void sendMessage(String key, Film value) {
        long startTime = System.currentTimeMillis();
        if (isAsync) { // Send asynchronously
            System.out.println("Sent message: (" + key + ", " + value.toString() + ")");
            producer.send(
                    new ProducerRecord<String, Film>(TOPIC, key, value),
                    (Callback) new FilmCallBack(startTime, key, value));
        } else { // Send synchronously
            try {
                final ProducerRecord<String, Film> film =
                        new ProducerRecord<String, Film>(TOPIC, key, value);
                RecordMetadata metadata = producer.send(film).get();
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
        ProducerFilm producer = new ProducerFilm(TOPIC, false);
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
                            // We must create object Film
                            System.out.println(" line: " + line);
                            JSONObject obj = new JSONObject(line);
                            if (obj.has("id")){
                                int movieId = Integer.parseInt(obj.get("id").toString());
                                String imdbId = obj.get("imdb_id").toString();
                                String title = obj.get("title").toString();
                                Boolean adult = Boolean.valueOf(obj.get("adult").toString());
                                /*
                                List<String> genres = new ArrayList<String>();
                                JSONArray film_genres = (JSONArray) obj.get("genres");
                                for (int n = 0; n < film_genres.length(); n++) {
                                    JSONObject object = film_genres.getJSONObject(n);
                                    genres.add(object.get("name").toString());
                                }*/
                                String genres = null;
                                JSONArray film_genres = (JSONArray) obj.get("genres");
                                if(film_genres.length() > 0){
                                    JSONObject object = film_genres.getJSONObject(0);
                                    genres = object.get("name").toString();
                                }
                                String original_language = obj.get("original_language").toString();
                                /*
                                List<String> production_companies = new ArrayList<String>();
                                JSONArray film_companies = (JSONArray) obj.get("production_companies");
                                for (int n = 0; n < film_companies.length(); n++) {
                                    JSONObject object = film_companies.getJSONObject(n);
                                    production_companies.add(object.get("name").toString());
                                }*/
                                String production_companies = null;
                                JSONArray film_companies = (JSONArray) obj.get("production_companies");
                                if(film_companies.length()>0){
                                    JSONObject object = film_companies.getJSONObject(0);
                                    production_companies = object.get("name").toString();
                                }
                                String release_date = obj.get("release_date").toString();
                                Integer runtime = 0;
                                if(!obj.isNull("runtime")){
                                    runtime = Integer.valueOf(obj.get("runtime").toString());
                                }
                                String status = obj.get("status").toString();
                                String director = "undefined";
                                if(obj.has("crew")){
                                    JSONArray film_crew = (JSONArray) obj.get("crew");
                                    for (int n = 0; n < film_crew.length(); n++) {
                                        JSONObject object = film_crew.getJSONObject(n);
                                        String department = object.get("department").toString();
                                        String job = object.get("job").toString();
                                        if ((department.equals("Directing")) && (job.equals("Director"))){
                                            director = object.get("name").toString();
                                        }
                                    }
                                }
                                Film film = new Film(movieId, imdbId, title, adult, genres,
                                    original_language, production_companies, release_date, runtime, status, director);
                                System.out.println(" film: " + film.toString());
                                //System.exit(1);
                                // Set key message equal movieId
                                producer.sendMessage(String.valueOf(movieId), film);
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

class FilmCallBack implements Callback {

    private long startTime;
    private String key;
    private Film message;

    public FilmCallBack(long startTime, String key, Film message) {
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
