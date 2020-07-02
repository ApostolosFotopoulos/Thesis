package executequeries;

public class AuthorRating implements Comparable<AuthorRating> {
   String name;
   String lastActive;
   Integer rating;
   
   public AuthorRating () {
        this.name = "-";
        this.lastActive = "-";
        this.rating = 0;
    }
    
    public AuthorRating (String name,String lastActive, int rating) {
        this.name = name;
        this.lastActive = lastActive;
        this.rating = rating;
    }

    public String getName () {
        return this.name;
    }
    
    public Integer getRating () {
        return this.rating;
    }
    
    public void incRating (int val) {
        this.rating = this.rating + val;
    }
    
    @Override
    public String toString( ) {
        String tName = this.name;
        tName = tName.replaceAll("[0123456789]", "");
        int tNameLength = tName.length();
        for (int j=0; j<40-tNameLength; j++) { 
            tName=tName.concat(" ");
        }
        
        String tRating = String.valueOf(this.rating);
        int tRatingLength = tRating.length();
        for (int j=0; j<40-tRatingLength; j++) { 
            tRating=tRating.concat(" ");
        }
        
        String str = "Name: " + tName + " ||| Rating: " + tRating + " ||| Last Active: " + this.lastActive;
        return str;
    }

    @Override
    public int compareTo(AuthorRating o) {
        return this.getRating().compareTo(o.getRating());
    }
}
