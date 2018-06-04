package pfm.pelis.kafka;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

public class Film {
    private int movieid;
    private String imdbid;
    private String title;
    private Boolean adult;
    //private List<String> genres;
    private String genres; // Save only first genre film
    private String original_language;
    //private List<String> production_companies;
    private String production_companies; // Save only first production_company
    private String release_date;
    private Integer runtime;
    private String status;
    private String director;

    public Film(){
    }

    public Film(int movieid, String imdbid, String title, Boolean adult, String genres,
                String original_language, String production_companies, String release_date,
               Integer runtime, String status, String director){
        this.movieid = movieid;
        this.imdbid = imdbid;
        this.title = title;
        this.adult = adult;
        this.genres = genres;
        this.original_language = original_language;
        this.production_companies = production_companies;
        this.release_date = release_date;
        this.runtime = runtime;
        this.status = status;
        this.director = director;
    }

    public int getMovieid(){
        return this.movieid;
    }
    public String getImdbid(){
        return this.imdbid;
    }
    public String getTitle(){
        return this.title;
    }
    public Boolean getAdult() { return this.adult; }
    public String getGenres() { return this.genres; }
    public String getOriginalLanguage() { return this.original_language; }
    public String getProductionCompanies() { return this.production_companies; }
    public String getReleaseDate() { return this.release_date; }
    public Integer getRuntime() { return this.runtime; }
    public String getStatus() { return this.status; }
    public String getDirector() { return this.director; }

    @Override
    public String toString(){
        return "Film(" + movieid + ", " + imdbid + ", " + title + ", "
                + Boolean.toString(adult) + ", " + genres + ", " + original_language + ", " +
                production_companies + ", " + release_date + ", " +
                Integer.toString(runtime) + ", " + status + ", " + director + ")";
    }
}
