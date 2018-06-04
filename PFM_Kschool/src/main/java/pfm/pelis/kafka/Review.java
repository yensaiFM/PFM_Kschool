package pfm.pelis.kafka;
import java.util.Date;

public class Review {
    private int userid;
    private int movieid;
    private float rating;
    private Long timestamp;

    public Review(){
    }

    public Review(int userid, int movieid, float rating, Long timestamp){
        this.userid = userid;
        this.movieid = movieid;
        this.rating = rating;
        this.timestamp = timestamp;
    }

    public int getUserid(){
        return this.userid;
    }
    public int getMovieid(){
        return this.movieid;
    }
    public float getRating(){ return this.rating; }
    public Long getTimestamp(){
        return this.timestamp;
    }

    @Override
    public String toString(){
        return "Review(" + userid + ", " + movieid + ", " + rating + ", " + timestamp + ")";
    }
}
