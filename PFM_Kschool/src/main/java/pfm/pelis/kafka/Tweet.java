package pfm.pelis.kafka;

public class Tweet {
    private String id;
    private String text;
    private String user;
    private Long timestamp;
    private String lang;
    private String hastag;


    public Tweet(){
    }

    public Tweet(String id, String text, String user, Long timestamp, String lang, String hastag){
        this.id = id;
        this.text = text;
        this.user = user;
        this.timestamp = timestamp;
        this.lang = lang;
        this.hastag = hastag;
    }

    public String getId() { return this.id; }
    public String getText(){
        return this.text;
    }
    public String getUser(){
        return this.user;
    }
    public Long getTimestamp(){
        return this.timestamp;
    }
    public String getLang() { return this.lang; }
    public String getHastag() { return this.hastag; }

    @Override
    public String toString(){
        return "Tweet(" + id + ", " +text + ", " + user + ", " + Long.toString(timestamp) + ", "
                + lang + ", " + hastag + ")";
    }
}
