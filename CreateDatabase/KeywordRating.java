package createdatabase;

public class KeywordRating implements Comparable<KeywordRating>{
    String keyword;
    Integer timesFound;
    
    public KeywordRating () {
        this.keyword = "-";
        this.timesFound = 0;
    }
    
    public KeywordRating (String keyword, int timesFound) {
        this.keyword = keyword;
        this.timesFound = timesFound;
    }
    
    public String getKeyword () {
        return this.keyword;
    }
    
    public Integer getTimesFound () {
        return this.timesFound;
    }

    @Override
    public String toString( ) {
        String tKeyword = this.keyword;
        int tKeywordLength = tKeyword.length();
        for (int j=0; j<15-tKeywordLength; j++) { 
            tKeyword=tKeyword.concat(" ");
        }
        
        String str = "Keyword: " + tKeyword + " ||| Times found: " + this.timesFound;
        return str;
    }
    
    @Override
    public int compareTo(KeywordRating o) {
        return this.getTimesFound().compareTo(o.getTimesFound());
    }
}
