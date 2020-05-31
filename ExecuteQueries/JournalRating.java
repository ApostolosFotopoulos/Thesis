package executequeries;

public class JournalRating implements Comparable<JournalRating> {
   String name;
   String lastActive;
   Integer rating;
   
   public JournalRating () {
        this.name = "-";
        this.lastActive = "-";
        this.rating = 0;
    }
    
    public JournalRating (String name, String lastActive, int rating) {
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
        int tNameLength = tName.length();
        for (int j=0; j<60-tNameLength; j++) { 
            tName=tName.concat(" ");
        }
        
        String tRating = String.valueOf(this.rating);
        int tRatingLength = tRating.length();
        for (int j=0; j<30-tRatingLength; j++) { 
            tRating=tRating.concat(" ");
        }
        
        String str = "Name: " + tName + " ||| Rating: " + tRating + " ||| Last Active: " + this.lastActive;
        return str;
    }
    
    @Override
    public int compareTo(JournalRating o) {
         return this.getRating().compareTo(o.getRating());
    }
}
