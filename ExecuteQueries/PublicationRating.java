package executequeries;

public class PublicationRating implements Comparable<PublicationRating> {
    String key;
    String title;
    String year;
    Integer rating;
    
    public PublicationRating () {
        this.key = "-";
        this.title = "-";
        this.year = "-";
        this.rating = 0;
    }
    
    public PublicationRating (String key, String title, String year, int rating) {
        this.key = key;
        this.title = title;
        this.year = year;
        this.rating = rating;
    }
    
    public void increaseRatingByOne () {
        this.rating = this.rating + 1;
    }
    
    public String getKey () {
        return this.key;
    }
    
    public Integer getRating () {
        return this.rating;
    }
    
    @Override
    public String toString( ) {
        String tTitle = this.title;
        int tTitleLength = tTitle.length();
        for (int j=0; j<120-tTitleLength; j++) { 
            tTitle=tTitle.concat(" ");
        }
        
        String str = "Title: " + tTitle + " ||| Rating: " + this.rating;
        return str;
    }

    @Override
    public int compareTo(PublicationRating o) {
        return this.getRating().compareTo(o.getRating());
    }
}
