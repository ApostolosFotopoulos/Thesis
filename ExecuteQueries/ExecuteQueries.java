package executequeries;

import java.awt.Desktop;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Scanner;

public class ExecuteQueries {
    
    //Used when we want to get some values from the database.
    public static ResultSet selectQuery(Connection conn,String queryStr) throws SQLException{
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(queryStr);
        return rs;
    }
    
    //Used when we want to change some values in the database.
    public static void editQuery(Connection conn,String queryStr) throws SQLException{
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(queryStr);
        }
    }
    
    //Prints a char x times.
    public static String printChar(String c, int times){
        String s = "";
        for (int i=0; i<times; i++) {
            s = s.concat(c);
        }
        return s;
    }
    
    //Used when showing the settings.
    public static void printEnabledOrDisabled(Boolean b){
        if (b) {
            System.out.println(" (Enabled)");
        }
        else {
            System.out.println(" (Disabled)");
        }
    }
    
    //Prints a string if its not null.
    public static void printIfNotNull(String pre, String s){
        if (!(s == null)) {
            System.out.println(pre+""+s.trim());
        }
    }
    
    //Capitalizes the 1st letter of every word in a string.
    private static String capitalize(String string) {
        char[] chars = string.toLowerCase().toCharArray();
        boolean found = false;
        for (int i = 0; i < chars.length; i++) {
            if (!found && Character.isLetter(chars[i])) {
                chars[i] = Character.toUpperCase(chars[i]);
                found = true;
            }
            else if (Character.isWhitespace(chars[i]) || chars[i]=='.' || chars[i]=='\'') {
                found = false;
            }
        }
        return String.valueOf(chars);
    }
    
    //Finds the value of a keyword. if a keyword contains 1 word the value is 1, if it contains 2 the value is 2 etc.
    private static int getValueOfKeyword(String keyword) {
        Boolean prevIsWhiteSpace = true;
        int value = 0;
        for (char c : keyword.toCharArray()) {
            if (!Character.isWhitespace(c) && prevIsWhiteSpace) {
                value++;
            }

            prevIsWhiteSpace = Character.isWhitespace(c);
        }
        return value;
    }
    
    
    //Removes the numbers from a string. Used when printing names that contain a number (Duplicate names have numbers next to them to distinguish them).
    public static String remNumbers(String s) {
        String str = s.replaceAll("[0123456789]", "");
        return str;
    }
    
    public static void main(String[] args) throws SQLException, URISyntaxException, IOException {
        Boolean enableUI = true;
        Boolean showDatabaseData = false;       //Shows the number of authors,articles,books etc.
        
        Boolean mostActiveAuthors    = false;   //Shows the most active authors.
        Boolean mostActiveJournals   = false;   //Shows the most active journals.
        Boolean mostActivePublishers = false;   //Shows the most active publishers.
        Boolean enableYear           = false;   //Shows the most active authors/journals/publishers in a specific year.
        String mostActiveInYear      = "2010";      //This is string is the year we want to look into.
        
        Boolean showMostUsedKeywords = false;  //Shows the most used keywords.
        
        Boolean searchByKeyword = false;         //Shows the publications the contain one specific keyword.
        String keywordToSearch = "communication"; //This string is the keyword that we want to look for.
        
        Boolean findDataOfPublication = false;  //Shows the authors of a publication. If there are multiple publications with the same title it returns all of them.
        Boolean openUrl               = false;  //Opens the url of the publication if one exists.
        String dataOfPublication      = "Knowledge Communication with Shared Databases.";  //This string is the title of the publication.
        
        Boolean findPublicationsOfAuthor = false;          //Shows the publications of a specific author. If there are multiple authors with the same name it returns all of them.
        String publicationsOfAuthor      = "Marco Di Renzo";  //This string is the name of the author.
        
        Boolean findClosestAuthorsToAuthor = false;  //Finds the authors that are the closest to an author. Works for multiple authors with the same name.
        String authorsCloseToAuthor        = "Marco Di Renzo";  //This string is the name of the author.
        
        Boolean findClosestJournalsToAuthor = false;          //Finds the journals that are the closest to an author. Works for multiple authors with the same name.
        String journalsCloseToAuthor        = "Marco Di Renzo";  //This string is the name of the author.
        
        Boolean findClosestPublishersToAuthor = false;  //Finds the publishers that are the closest to an author. Works for multiple authors with the same name.
        String publishersCloseToAuthor        = "Marco Di Renzo";  //This string is the name of the author.
        
        Boolean findMostUsedKeywordsOfAuthor = false;  //Finds the most used keywords of an author. Works for multiple authors with the same name.
        String keywordsOfAuthor              = "Marco Di Renzo";  //This string is the name of the author.
        
        Boolean searchBySetOfKeywords     = false;     //Finds the most relevant publications to a set of keywords.
        Boolean removeMostUsedKeywords    = false;    //Removes the most used keywords from the set of keywords the user has entered.
        Boolean printMostRelevantArticles = true;  //Prints the most relevant publications to the set of keywords.
        String[] keywords = {"Knowledge Communication","Shared Databases"};  //This array of strings is the set of keywords.
        
        Boolean findBestJournalForArticle       = true; /*Finds the the most relevant journals to the set of keywords. searchBySetOfKeywords needs to be enabled
                                                          for this to work because it uses the list of the most relevant publications to the set of keywords.*/
        Boolean findBestPublisherForPublication = true; /*Finds the the most relevant publishers to the set of keywords. searchBySetOfKeywords needs to be enabled
                                                          for this to work because it uses the list of the most relevant publications to the set of keywords.*/
        Boolean findBestAuthorForArticle        = true; /*Finds the the most relevant authors to the set of keywords. searchBySetOfKeywords needs to be enabled
                                                          for this to work because it uses the list of the most relevant publications to the set of keywords.*/ 
         
        final int ARRAY_LENGTH = 1000;    /*The length of the lists that contain the most relevant articles,authors,journals to the set of keywords.
                                            The bigger the length the better the precision of the results.*/
        int NUMBER_OF_RESULTS = 10;  //The ammount of results printed in most of the above queries.
        int MIN_KEYWORD_LENGTH  = 5;  //The minimum length of a keyword. Used by all the queries that use keywords.
        String url =  "jdbc:neo4j:bolt://localhost";
        Connection conn = null;
        Inflector inf = new Inflector();  //Used to change words from singular to plural and the opposite.
        ResultSet rs;
          
        //System.out.println(inf.singularize("children"));
        //System.out.println(inf.pluralize("children"));
        //Establishes connection with the database.
        try {
            conn = DriverManager.getConnection(url, "neo4j", "1234");
        } catch (SQLException e) {
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }
        
        if (enableUI) {
            //Queries
            showDatabaseData                = false;
            mostActiveAuthors               = false;
            mostActiveJournals              = false;
            mostActivePublishers            = false;
            showMostUsedKeywords            = false;
            searchByKeyword                 = false;   
            findDataOfPublication           = false;
            findPublicationsOfAuthor        = false;
            findClosestAuthorsToAuthor      = false;
            findClosestJournalsToAuthor     = false;
            findClosestPublishersToAuthor   = false;
            findMostUsedKeywordsOfAuthor    = false;
            searchBySetOfKeywords           = false;
            findBestJournalForArticle       = false;
            findBestPublisherForPublication = false;
            findBestAuthorForArticle        = false;

            //Settings
            enableYear                = true;
            openUrl                   = true; 
            removeMostUsedKeywords    = true;
            printMostRelevantArticles = true;
            
            System.out.println("Actions:");
        }
        while(true) {
            if (enableUI) {
                System.out.println("1)  Settings");
                System.out.println("2)  Show database data");
                System.out.println("3)  Show most active authors, journals and publishers.");
                System.out.println("4)  Show most used keywords");
                System.out.println("5)  Find the data related to a publication");
                System.out.println("6)  Find the publications of an author");
                System.out.println("7)  Find the closest authors, journals and publishers to an author");
                System.out.println("8)  Find the most used keywords by an author");
                System.out.println("9)  Find the most relevant publications, journals, publishers and authors to a set of keywords");
                System.out.println("10) Exit");
                System.out.print("Action: ");
                
                Scanner in = new Scanner(System.in);
                String action = in.nextLine();
                System.out.println();
                
                if (action.equals("1")) {
                    System.out.println("Settings:");
                    while(true) {
                        System.out.print("1) Enable/Disable the year specification in all the \"most active\" queries");
                        printEnabledOrDisabled(enableYear);
                        System.out.print("2) Enable/Disable the opening of the url when searching for a specific publication");
                        printEnabledOrDisabled(openUrl);
                        System.out.print("3) Enable/Disable the removal of the most frequent keywords from the set of keywords that was entered");
                        printEnabledOrDisabled(removeMostUsedKeywords);
                        System.out.print("4) Enable/Disable the printing of the most relevant publications to a set of keywords");
                        printEnabledOrDisabled(printMostRelevantArticles);
                        System.out.print("5) Change the number of the results shown in most of the queries");
                        System.out.println(" ("+NUMBER_OF_RESULTS+")");
                        System.out.print("6) Change the minimum length of a keyword");
                        System.out.println(" ("+MIN_KEYWORD_LENGTH+")");
                        System.out.println("7) Back");
                        
                        System.out.print("Change setting: ");
                        action = in.nextLine();
                        System.out.println();
                        
                        if (action.equals("1")) {
                            enableYear = !enableYear;
                        }
                        else if (action.equals("2")) {
                            openUrl = !openUrl;
                        }
                        else if (action.equals("3")) {
                            removeMostUsedKeywords = !removeMostUsedKeywords;
                        }
                        else if (action.equals("4")) {
                            printMostRelevantArticles = !printMostRelevantArticles;
                        }
                        else if (action.equals("5")) {                          
                            while (true) {
                                System.out.print("New value: ");
                                try{
                                    String newVal = in.nextLine();
                                    NUMBER_OF_RESULTS = Integer.parseInt(newVal);
                                    break;
                                }catch (NumberFormatException ex) {
                                   System.out.println("The number needs to be an integer.");
                                }
                            }
                            
                        }
                        else if (action.equals("6")) {                          
                            while (true) {
                                System.out.print("New value: ");
                                try{
                                    String newVal = in.nextLine();
                                    MIN_KEYWORD_LENGTH = Integer.parseInt(newVal);
                                    break;
                                }catch (NumberFormatException ex) {
                                   System.out.println("The number needs to be an integer.");
                                }
                            }
                            
                        }
                        else if (action.equals("7") || action.equals("e") || action.toLowerCase().endsWith("exit") || action.equals("b") ||action.toLowerCase().endsWith("back")) {
                            break;
                        }
                        else {
                            System.out.println("Invalid action.");
                        }   
                    }
                }
                else if (action.equals("2")) {
                    showDatabaseData = true;
                }
                else if (action.equals("3")) {
                    mostActiveAuthors = true;
                    mostActiveJournals = true;
                    mostActivePublishers = true;
                    if (enableYear) {
                        System.out.print("Year: ");
                        mostActiveInYear = in.nextLine();
                    }
                }
                else if (action.equals("4")) {
                    showMostUsedKeywords = true;
                }
                else if (action.equals("5")) {
                    findDataOfPublication = true;
                    System.out.print("Find the data of the publication with title: ");
                    dataOfPublication = in.nextLine();
                }
                else if (action.equals("6")) {
                    findPublicationsOfAuthor = true;
                    System.out.print("Find the publications of the author named: ");
                    publicationsOfAuthor = in.nextLine();
                }
                else if (action.equals("7")) {
                    findClosestAuthorsToAuthor = true;
                    findClosestJournalsToAuthor = true;
                    findClosestPublishersToAuthor = true;
                    System.out.print("Find the closest authors, journals and publishers to the author named: ");
                    authorsCloseToAuthor = in.nextLine();
                    journalsCloseToAuthor = authorsCloseToAuthor;
                    publishersCloseToAuthor = authorsCloseToAuthor;
                }
                else if (action.equals("8")) {
                    findMostUsedKeywordsOfAuthor = true;
                    System.out.print("Find the most used keywords by the author named: ");
                    keywordsOfAuthor = in.nextLine();
                }
                else if (action.equals("9")) {
                    searchBySetOfKeywords = true;
                    findBestJournalForArticle = true;
                    findBestPublisherForPublication = true;
                    findBestAuthorForArticle = true;                 
                    keywords = new String[100];
                    
                    for (int i = 0; i < keywords.length; i++) {
                        keywords[i] = "-";
                    }
                    
                    System.out.println("Enter keywords and type exit when finished.");
                    for (int i = 0; i < keywords.length; i++) {
                        System.out.print("Add keyword: ");
                        String tmp = in.nextLine();
                        if (tmp.equals("e") || tmp.toLowerCase().endsWith("exit")) {
                            break;
                        }
                        keywords[i] = tmp;
                    }
                }
                else if (action.equals("10") || action.equals("e") || action.toLowerCase().endsWith("exit")){
                    conn.close();
                    break;
                }
                else {
                    System.out.println("Invalid action.");
                }
            }
                        
            ////////////////////////////////////////// Shows amount of data in the database. //////////////////////////////////////////////////////////////
            if (showDatabaseData) {
                //Publications
                rs = selectQuery(conn,"match (a)-[:IS]-(b:publication) return count(a) as count");
                String numberOfPublications = "0";
                if (rs.next()) {
                    numberOfPublications = rs.getString("count");
                }

                rs = selectQuery(conn,"match (a:article) return count(a) as count");
                String numberOfArticles = "0";
                if (rs.next()) {
                    numberOfArticles = rs.getString("count");
                }

                rs = selectQuery(conn,"match (a:incollection) return count(a) as count");
                String numberOfIncollections = "0";
                if (rs.next()) {
                    numberOfIncollections = rs.getString("count");
                }

                rs = selectQuery(conn,"match (a:inproceedings) return count(a) as count");
                String numberOfInproceedings = "0";
                if (rs.next()) {
                    numberOfInproceedings = rs.getString("count");
                }

                rs = selectQuery(conn,"match (a:proceedings) return count(a) as count");
                String numberOfProceedings = "0";
                if (rs.next()) {
                    numberOfProceedings = rs.getString("count");
                }

                rs = selectQuery(conn,"match (a:book) return count(a) as count");
                String numberOfBooks = "0";
                if (rs.next()) {
                    numberOfBooks = rs.getString("count");
                }

                rs = selectQuery(conn,"match (a:mastersthesis) return count(a) as count");
                String numberOfMastersthesis = "0";
                if (rs.next()) {
                    numberOfMastersthesis = rs.getString("count");
                }

                rs = selectQuery(conn,"match (a:phdthesis) return count(a) as count");
                String numberOfPhdthesis = "0";
                if (rs.next()) {
                    numberOfPhdthesis = rs.getString("count");
                }

                //Authors
                rs = selectQuery(conn,"match (a:author) return count(a) as count");
                String numberOfAuthors = "0";
                if (rs.next()) {
                    numberOfAuthors = rs.getString("count");
                }

                //Journals
                rs = selectQuery(conn,"match (a:journal) return count(a) as count");
                String numberOfJournals = "0";
                if (rs.next()) {
                    numberOfJournals = rs.getString("count");
                }

                //Publishers
                rs = selectQuery(conn,"match (a:publisher) return count(a) as count");
                String numberOfPublishers = "0";
                if (rs.next()) {
                    numberOfPublishers = rs.getString("count");
                }

                //Keywords
                rs = selectQuery(conn,"match (a:keyword) return count(a) as count");
                String numberOfkeywords = "0";
                if (rs.next()) {
                    numberOfkeywords = rs.getString("count");
                }

                System.out.println("Ammount of data in the database:");
                System.out.println("Publications:    "+numberOfPublications);
                System.out.println("Articles:        "+numberOfArticles);
                System.out.println("Incollections:   "+numberOfIncollections);
                System.out.println("Inproceedings:   "+numberOfInproceedings);
                System.out.println("Books:           "+numberOfBooks);
                System.out.println("Proceedings:     "+numberOfProceedings);
                System.out.println("PhD theses:      "+numberOfPhdthesis);
                System.out.println("Master's theses: "+numberOfMastersthesis);
                System.out.println("Authors:         "+numberOfAuthors);
                System.out.println("Journals:        "+numberOfJournals);
                System.out.println("Publishers:      "+numberOfPublishers);
                System.out.println("Keywords:        "+numberOfkeywords);
                System.out.println();
                showDatabaseData = false;
            }

            ////////////////////////////////////////// Finds the most active authors. //////////////////////////////////////////////////////////////
            if (mostActiveAuthors) {
                if (enableYear) {
                    rs = selectQuery(conn,"match (a:author)-[:WROTE]-(b {year:'"+mostActiveInYear+"'})"
                                         +" return a.name,count(b) as publications order by publications desc limit "+NUMBER_OF_RESULTS);
                }
                else {
                    rs = selectQuery(conn,"match (a:author)-[:WROTE]-(b)"
                                         +" return a.name,count(b) as publications order by publications desc limit "+NUMBER_OF_RESULTS);
                }

                if (rs.next()) {
                    if (enableYear) {
                        System.out.println("Top "+NUMBER_OF_RESULTS+" most active authors in "+mostActiveInYear+":");
                    }
                    else {
                        System.out.println("Top "+NUMBER_OF_RESULTS+" most active authors:");
                    }

                    System.out.println("Author "+printChar(" ",39)+"Publications");
                    int i=1;
                    do {
                        String name = rs.getString("a.name").trim();
                        name = remNumbers(name);
                        int tLength = name.length();
                        int numLength = String.valueOf(i).length();
                        for (int j=0; j<50-tLength-numLength; j++) { 
                            name=name.concat(" ");
                        }

                        System.out.print(i+")"+name);
                        String publications = rs.getString("publications");
                        System.out.println(publications);
                        i++;
                    } while(rs.next());
                    System.out.println();
                }
                else {
                    if (enableYear) {
                        System.out.println("No authors were active in "+mostActiveInYear+".");
                    }
                    else {
                        System.out.println("No authors exist in the database.");
                    }
                    System.out.println();
                }
                mostActiveAuthors = false;
            }

            ////////////////////////////////////////// Finds the most active journals. //////////////////////////////////////////////////////////////
            if (mostActiveJournals) {
                //Journals are only associated with articles.
                if (enableYear) {
                    rs = selectQuery(conn,"match (a:journal)-[:INCLUDES]-(b:article {year:'"+mostActiveInYear+"'})"
                                         +" return a.name,count(b) as articles order by articles desc limit "+NUMBER_OF_RESULTS);
                }
                else {
                    rs = selectQuery(conn,"match (a:journal)-[:INCLUDES]-(b:article)"
                                         +" return a.name,count(b) as articles order by articles desc limit "+NUMBER_OF_RESULTS);
                }

                if (rs.next()) {
                    if (enableYear) {
                        System.out.println("Top "+NUMBER_OF_RESULTS+" most active journals in "+mostActiveInYear+":");
                    }
                    else {
                        System.out.println("Top "+NUMBER_OF_RESULTS+" most active journals:");
                    }

                    System.out.println("Journal "+printChar(" ",70)+"Articles");
                    int i=1;
                    do {
                        String name = rs.getString("a.name").trim();
                        int tLength = name.length();
                        int numLength = String.valueOf(i).length();
                        for (int j=0; j<80-tLength-numLength; j++) { 
                            name=name.concat(" ");
                        }
                        System.out.print(i+")"+name);
                        String articles = rs.getString("articles");
                        System.out.println(articles);
                        i++;
                    } while(rs.next());
                    System.out.println();
                }
                else {
                    if (enableYear) {
                        System.out.println("No journals were active in "+mostActiveInYear+".");
                    }
                    else {
                        System.out.println("No journals exist in the database.");
                    }
                    System.out.println();
                }
                mostActiveJournals = false;
            }

            ////////////////////////////////////////// Finds the most active publishers. //////////////////////////////////////////////////////////////
            if (mostActivePublishers) {
                if (enableYear) {
                    rs = selectQuery(conn,"match (a:publisher)-[:PUBLISHED]-(b {year:'"+mostActiveInYear+"'})"
                                         +" return a.name,count(b) as publications order by publications desc limit "+NUMBER_OF_RESULTS);
                }
                else {
                    rs = selectQuery(conn,"match (a:publisher)-[:PUBLISHED]-(b)"
                    +" return a.name,count(b) as publications order by publications desc limit "+NUMBER_OF_RESULTS);
                }

                if (rs.next()) {
                    if (enableYear) {
                        System.out.println("Top "+NUMBER_OF_RESULTS+" most active publishers in "+mostActiveInYear+":");
                    }
                    else {
                        System.out.println("Top "+NUMBER_OF_RESULTS+" most active publishers:");
                    }

                    System.out.println("Publisher "+printChar(" ",66)+"Publications");
                    int i=1;
                    do {
                        String name = rs.getString("a.name").trim();
                        int tLength = name.length();
                        int numLength = String.valueOf(i).length();
                        for (int j=0; j<80-tLength-numLength; j++) { 
                            name=name.concat(" ");
                        }
                        System.out.print(i+")"+name);
                        String publications = rs.getString("publications");
                        System.out.println(publications);
                        i++;
                    } while(rs.next());
                    System.out.println();
                }
                else {            
                    if (enableYear) {
                        System.out.println("No publishers were active in "+mostActiveInYear+".");
                    }
                    else {
                        System.out.println("No publishers exist in the database.");
                    }
                    System.out.println();
                }
                mostActivePublishers = false;
            }

            ///////////////////////////////////////////// Shows the most used keywords. //////////////////////////////////////////////////////////////
            if (showMostUsedKeywords) {
                rs = selectQuery(conn,"match (a:keyword) return a.keyword,a.timesFound as timesFound order by timesFound desc limit "+NUMBER_OF_RESULTS);
                if (rs.next()) {
                   System.out.println("Most used keywords:");
                   System.out.println("Keyword "+printChar(" ",39)+"Occurrences");
                   int i=1;
                   do {
                        String keyword = rs.getString("a.keyword").trim();
                        int tLength = keyword.length();
                        int numLength = String.valueOf(i).length();
                        for (int j=0; j<50-tLength-numLength; j++) { 
                            keyword=keyword.concat(" ");
                        }  
                        System.out.print(i+")"+keyword);
                        String timesFound = rs.getString("timesFound").trim();
                        System.out.println(timesFound);
                        i++;
                    } while(rs.next());
                    System.out.println();
                }
                else {
                    System.out.println("No keywords exist.");
                    System.out.println(); 
                }
                showMostUsedKeywords = false;
            }

            /////////////////////////////////////// Finds all the publications containing a specific keyword. //////////////////////////////////////////////////
            if (searchByKeyword) {
                keywordToSearch = inf.singularize(keywordToSearch.toLowerCase());
                keywordToSearch = keywordToSearch.replaceAll("[.,:]", "");
                rs = selectQuery(conn,"match (a)-[:IS]-(b:publication) WHERE toLower(a.title) CONTAINS '"+keywordToSearch+"' OR toLower(a.title) CONTAINS '"+inf.pluralize(keywordToSearch)+"'"
                                     +" return a.key,a.title,a.year,labels(a),a.ee,a.school limit "+NUMBER_OF_RESULTS);
                if (rs.next()) {
                    System.out.println("Publications containing the keyword \""+keywordToSearch+"\":");
                    int i=1;
                    do {
                        String key = rs.getString("a.key").trim();        
                        System.out.println(printChar("/",50)+" "+i+". "+printChar("/",50));

                        String title = rs.getString("a.title");
                        String year = rs.getString("a.year");
                        String ee = rs.getString("a.ee");
                        String school = rs.getString("a.school");
                        java.sql.Array arr = rs.getArray("labels(a)");
                        String[] str = (String[])arr.getArray();
                        String type = str[0].trim();

                        printIfNotNull("Type:       ",capitalize(type));
                        printIfNotNull("Title:      ",title);
                        printIfNotNull("Year:       ",year);
                        printIfNotNull("Url:        ",ee);
                        printIfNotNull("School:     ",school);

                        if (type.equals("article")) {
                            ResultSet rs2 = selectQuery(conn,"match (a:journal)-[:INCLUDES]-(b:article {key: '"+key+"'}) return a.name");
                            if (rs2.next()) {
                                String journal = rs2.getString("a.name");
                                printIfNotNull("Journal:    ",journal);
                            } 
                        }

                        if (type.equals("article") || type.equals("book") || type.equals("proceedings") || type.equals("phdthesis")) {
                            ResultSet rs2 = selectQuery(conn,"match (a:publisher)-[:PUBLISHED]-(b {key: '"+key+"'}) return a.name");    
                            if (rs2.next()) {
                                String publisher = rs2.getString("a.name");
                                printIfNotNull("Publisher:  ",publisher);
                            } 
                        }

                        i++;
                    } while(rs.next());
                    System.out.println(printChar("/",50)+"////"+printChar("/",50));
                    System.out.println();
                }
                else {
                    System.out.println("There are no publications containing this keyword.");
                    System.out.println(); 
                }
                searchByKeyword = false;
            }

            ///////////////////////////////// Finds the data associated with a specific publication. (Publications can have the same name) //////////////////////////////////
            if (findDataOfPublication) {
                rs = selectQuery(conn,"match (a:author)-[:WROTE]-(b) WHERE toLower(b.title) CONTAINS '"+dataOfPublication.toLowerCase()+"'"
                                     +" return a.name,b.key,b.title,labels(b),b.year,b.month,b.number,b.volume,b.booktitle,b.series,b.pages,b.isbn,b.ee order by b.key");
                if (rs.next()) {
                    System.out.println("Publications with the title \""+dataOfPublication+"\":");
                    String prev="-";
                    int i=1;
                    do {
                        String name = rs.getString("a.name");
                        String key = rs.getString("b.key").trim();

                        if (!prev.equals(key)) {
                            System.out.println(printChar("/",50)+" "+i+". "+printChar("/",50));
                            String title = rs.getString("b.title");
                            String year = rs.getString("b.year");
                            String month = rs.getString("b.month");
                            String number = rs.getString("b.number");
                            String volume = rs.getString("b.volume");
                            String booktitle = rs.getString("b.booktitle");
                            String series = rs.getString("b.series");
                            String pages = rs.getString("b.pages");
                            String isbn = rs.getString("b.isbn");
                            String ee = rs.getString("b.ee");
                            java.sql.Array arr = rs.getArray("labels(b)");
                            String[] str = (String[])arr.getArray();
                            String type = str[0].trim();

                            printIfNotNull("Type:       ",capitalize(type));
                            printIfNotNull("Title:      ",title);
                            printIfNotNull("Month:      ",month);
                            printIfNotNull("Year:       ",year);
                            printIfNotNull("Volume:     ",volume);
                            printIfNotNull("Number:     ",number);
                            printIfNotNull("Pages:      ",pages);
                            printIfNotNull("Booktitle:  ",booktitle);
                            printIfNotNull("Series:     ",series);
                            printIfNotNull("Isbn:       ",isbn);
                            printIfNotNull("Url:        ",ee);

                            if (!(ee == null) && openUrl) {
                                if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
                                    Desktop.getDesktop().browse(new URI(ee));
                                }
                            }

                            if (type.equals("article")) {
                                ResultSet rs2 = selectQuery(conn,"match (a:journal)-[:INCLUDES]-(b:article {key: '"+key+"'}) return a.name");
                                if (rs2.next()) {
                                    String journal = rs2.getString("a.name");
                                    printIfNotNull("Journal:    ",journal);
                                } 
                            }

                            if (type.equals("article") || type.equals("book") || type.equals("proceedings") || type.equals("phdthesis")) {
                                ResultSet rs2 = selectQuery(conn,"match (a:publisher)-[:PUBLISHED]-(b {key: '"+key+"'}) return a.name");    
                                if (rs2.next()) {
                                    String publisher = rs2.getString("a.name");
                                    printIfNotNull("Publisher:  ",publisher);
                                } 
                            }

                            i++;
                        }
                        prev = key;
                        printIfNotNull("Author:     ",remNumbers(name));
                    } while(rs.next());
                    System.out.println(printChar("/",50)+"////"+printChar("/",50));
                    System.out.println();
                }
                else {
                    System.out.println("The publication does not exist.");
                    System.out.println(); 
                }
                findDataOfPublication = false;
            }

            //////////////////////////////// Finds the publications of a specific author. (Authors can have the same name) /////////////////////////////////////
            if (findPublicationsOfAuthor) {
                publicationsOfAuthor = publicationsOfAuthor.toLowerCase();
                rs = selectQuery(conn,"match (a:author)-[:WROTE]-(b) WHERE toLower(a.name) CONTAINS '"+publicationsOfAuthor+"'"
                                     +" return id(a),b.key,b.title,b.year,labels(b),b.ee,b.school order by id(a)");
                publicationsOfAuthor = capitalize(publicationsOfAuthor);

                if (rs.next()) {
                    String prev="-";
                    int i=1;
                    System.out.println("The author named \""+publicationsOfAuthor+"\" participated in the writing of these publications:");
                    do {
                        String id = rs.getString("id(a)").trim();
                        String title = rs.getString("b.title");
                        String year = rs.getString("b.year");
                        if (!prev.equals(id)) {
                            if (i>2) {
                                i=1;
                                System.out.println(printChar("/",50)+"////"+printChar("/",50));
                                System.out.println();
                                System.out.println("There are multiple authors named \""+publicationsOfAuthor+"\".");
                                System.out.println("The author named \""+publicationsOfAuthor+"\" participated in the writing of these publications:");
                            }  
                        }
                        String key = rs.getString("b.key").trim();        
                        System.out.println(printChar("/",50)+" "+i+". "+printChar("/",50));

                        String ee = rs.getString("b.ee");
                        String school = rs.getString("b.school");
                        java.sql.Array arr = rs.getArray("labels(b)");
                        String[] str = (String[])arr.getArray();
                        String type = str[0].trim();

                        printIfNotNull("Type:       ",capitalize(type));
                        printIfNotNull("Title:      ",title);
                        printIfNotNull("Year:       ",year);
                        printIfNotNull("Url:        ",ee);
                        printIfNotNull("School:     ",school);

                        if (type.equals("article")) {
                            ResultSet rs2 = selectQuery(conn,"match (a:journal)-[:INCLUDES]-(b:article {key: '"+key+"'}) return a.name");
                            if (rs2.next()) {
                                String journal = rs2.getString("a.name");
                                printIfNotNull("Journal:    ",journal);
                            } 
                        }

                        if (type.equals("article") || type.equals("book") || type.equals("proceedings") || type.equals("phdthesis")) {
                            ResultSet rs2 = selectQuery(conn,"match (a:publisher)-[:PUBLISHED]-(b {key: '"+key+"'}) return a.name");    
                            if (rs2.next()) {
                                String publisher = rs2.getString("a.name");
                                printIfNotNull("Publisher:  ",publisher);
                            } 
                        }

                        prev = id;
                        i++;
                    } while(rs.next());
                    System.out.println(printChar("/",50)+"////"+printChar("/",50));
                    System.out.println();
                }
                else {
                    System.out.println("The author does not exist.");
                    System.out.println(); 
                }
                findPublicationsOfAuthor = false;
            }

            //////////////////////////////// Find the authors that are closest to an author. (Authors can have the same name) ////////////////////////////////
            if (findClosestAuthorsToAuthor) {
                ArrayList<AuthorRating> authors = new ArrayList<>();
                for (int i=0; i<ARRAY_LENGTH;i++) {
                    authors.add(i,new AuthorRating());
                }

                authorsCloseToAuthor = authorsCloseToAuthor.toLowerCase();
                rs = selectQuery(conn,"match (a:author)-[:WROTE]-(b)-[:WROTE]-(c:author) WHERE toLower(a.name) CONTAINS '"+authorsCloseToAuthor+"'"
                                     +" return id(a),c.name order by id(a)");
                authorsCloseToAuthor = capitalize(authorsCloseToAuthor);

                if (rs.next()) {
                    String prev="-";
                    int k=0;
                    do {
                        String id = rs.getString("id(a)").trim();
                        String authorName = rs.getString("c.name").trim();

                        if (!prev.equals(id) && k>0) { 
                            System.out.println("The author named \""+remNumbers(authorsCloseToAuthor)+"\" is mostly associated with these authors:");
                            int l=1;
                            for (int i=ARRAY_LENGTH-1; i>-1; i--) {
                                String name = authors.get(i).getName();
                                if (!name.equals("-")) {
                                    int lLength = String.valueOf(l).length();
                                    System.out.println(l+")"+printChar(" ",3-lLength)+""+authors.get(i));
                                }
                                if (l==NUMBER_OF_RESULTS) {
                                    break;
                                }
                                l++;
                            }
                            System.out.println();
                            System.out.println("There are multiple authors named \""+authorsCloseToAuthor+"\".");

                            authors.clear();
                            for (int i=0; i<ARRAY_LENGTH;i++) {
                                authors.add(i,new AuthorRating());
                            }
                        }

                        Boolean authorExists = false;
                        for (int j=0; j<ARRAY_LENGTH; j++) {
                            if (authors.get(j).getName().equals(authorName)) {
                                authorExists = true;
                                authors.get(j).incRating(1);
                            }
                        }

                        if (!authorExists) {
                            ResultSet rs2 = selectQuery(conn, "match (a:author {name: '"+authorName+"'})-[:WROTE]-(n) RETURN DISTINCT n.year order by n.year DESC");
                            if (rs2.next()) {
                                String lastActive = rs2.getString("n.year").trim();
                                authors.set(0,new AuthorRating(authorName,lastActive,1)); 
                            }   
                        }

                        Collections.sort(authors);  
                        prev = id;
                        k++;
                    } while(rs.next());

                    System.out.println("The author named \""+remNumbers(authorsCloseToAuthor)+"\" is mostly associated with these authors:");
                    int l=1;
                    for (int i=ARRAY_LENGTH-1; i>-1; i--) {
                        String name = authors.get(i).getName();
                        if (!name.equals("-")) {
                            int lLength = String.valueOf(l).length();
                            System.out.println(l+")"+printChar(" ",3-lLength)+""+authors.get(i));
                        }
                        if (l==NUMBER_OF_RESULTS) {
                            break;
                        }
                        l++;
                    }
                    System.out.println();
                }
                else {
                    System.out.println("The author does not exist or isn't associated with any other authors.");
                    System.out.println(); 
                }
                findClosestAuthorsToAuthor = false;
            }

            //////////////////////////////// Find the journals that are closest to an author. (Authors can have the same name) ////////////////////////////////
            if (findClosestJournalsToAuthor) {
                ArrayList<JournalRating> journals = new ArrayList<>();
                for (int i=0; i<ARRAY_LENGTH;i++) {
                    journals.add(i,new JournalRating());
                }

                journalsCloseToAuthor = journalsCloseToAuthor.toLowerCase();
                //Journals are only associated with articles.
                rs = selectQuery(conn,"match (a:author)-[:WROTE]-(b:article)-[:INCLUDES]-(c:journal) WHERE toLower(a.name) CONTAINS '"+journalsCloseToAuthor+"'"
                                     +" return id(a),c.name order by id(a)");
                journalsCloseToAuthor = capitalize(journalsCloseToAuthor);

                if (rs.next()) {
                    String prev="-";
                    int k=0;
                    do {
                        String id = rs.getString("id(a)").trim();
                        String journalName = rs.getString("c.name").trim();

                        if (!prev.equals(id) && k>0) { 
                            System.out.println("The author named \""+remNumbers(journalsCloseToAuthor)+"\" is mostly associated with these journals:");
                            int l=1;
                            for (int i=ARRAY_LENGTH-1; i>-1; i--) {
                                String name = journals.get(i).getName();
                                if (!name.equals("-")) {
                                    int lLength = String.valueOf(l).length();
                                    System.out.println(l+")"+printChar(" ",3-lLength)+""+journals.get(i));
                                }
                                if (l==NUMBER_OF_RESULTS) {
                                    break;
                                }
                                l++;
                            }
                            System.out.println();
                            System.out.println("There are multiple authors named \""+journalsCloseToAuthor+"\".");

                            journals.clear();
                            for (int i=0; i<ARRAY_LENGTH;i++) {
                                journals.add(i,new JournalRating());
                            }
                        }

                        Boolean journalExists = false;
                        for (int j=0; j<ARRAY_LENGTH; j++) {
                            if (journals.get(j).getName().equals(journalName)) {
                                journalExists = true;
                                journals.get(j).incRating(1);
                            }
                        }

                        if (!journalExists) {
                            ResultSet rs2 = selectQuery(conn, "match (a:journal {name: '"+journalName+"'})-[:INCLUDES]-(n:article) RETURN DISTINCT n.year order by n.year DESC");
                            if (rs2.next()) {
                                String lastActive = rs2.getString("n.year").trim();
                                journals.set(0,new JournalRating(journalName,lastActive,1)); 
                            }   
                        }

                        Collections.sort(journals);  
                        prev = id;
                        k++;
                    } while(rs.next());

                    System.out.println("The author named \""+remNumbers(journalsCloseToAuthor)+"\" is mostly associated with these journals:");
                    int l=1;
                    for (int i=ARRAY_LENGTH-1; i>-1; i--) {
                        String name = journals.get(i).getName();
                        if (!name.equals("-")) {
                            int lLength = String.valueOf(l).length();
                            System.out.println(l+")"+printChar(" ",3-lLength)+""+journals.get(i));
                        }
                        if (l==NUMBER_OF_RESULTS) {
                            break;
                        }
                        l++;
                    }
                    System.out.println();
                }
                else {
                    System.out.println("The author does not exist or isn't associated with any journals.");
                    System.out.println(); 
                }
                findClosestJournalsToAuthor = false;
            }

            //////////////////////////////// Find the publishers that are closest to an author. (Authors can have the same name) ////////////////////////////////
            if (findClosestPublishersToAuthor) {
                ArrayList<PublisherRating> publishers = new ArrayList<>();
                for (int i=0; i<ARRAY_LENGTH;i++) {
                    publishers.add(i,new PublisherRating());
                }

                publishersCloseToAuthor = publishersCloseToAuthor.toLowerCase();
                rs = selectQuery(conn,"match (a:author)-[:WROTE]-(b)-[:PUBLISHED]-(c:publisher) WHERE toLower(a.name) CONTAINS '"+publishersCloseToAuthor+"'"
                                     +" return id(a),c.name order by id(a)");
                publishersCloseToAuthor = capitalize(publishersCloseToAuthor);

                if (rs.next()) {
                    String prev="-";
                    int k=0;
                    do {
                        String id = rs.getString("id(a)").trim();
                        String publisherName = rs.getString("c.name").trim();

                        if (!prev.equals(id) && k>0) { 
                            System.out.println("The author named \""+remNumbers(publishersCloseToAuthor)+"\" is mostly associated with these publishers:");
                            int l=1;
                            for (int i=ARRAY_LENGTH-1; i>-1; i--) {
                                String name = publishers.get(i).getName();
                                if (!name.equals("-")) {
                                    int lLength = String.valueOf(l).length();
                                    System.out.println(l+")"+printChar(" ",3-lLength)+""+publishers.get(i));
                                }
                                if (l==NUMBER_OF_RESULTS) {
                                    break;
                                }
                                l++;
                            }
                            System.out.println();
                            System.out.println("There are multiple authors named \""+publishersCloseToAuthor+"\".");

                            publishers.clear();
                            for (int i=0; i<ARRAY_LENGTH;i++) {
                                publishers.add(i,new PublisherRating());
                            }
                        }

                        Boolean publisherExists = false;
                        for (int j=0; j<ARRAY_LENGTH; j++) {
                            if (publishers.get(j).getName().equals(publisherName)) {
                                publisherExists = true;
                                publishers.get(j).incRating(1);
                            }
                        }

                        if (!publisherExists) {
                            ResultSet rs2 = selectQuery(conn, "match (a:publisher {name: '"+publisherName+"'})-[:PUBLISHED]-(n) RETURN DISTINCT n.year order by n.year DESC");
                            if (rs2.next()) {
                                String lastActive = rs2.getString("n.year").trim();
                                publishers.set(0,new PublisherRating(publisherName,lastActive,1)); 
                            }   
                        }

                        Collections.sort(publishers);  
                        prev = id;
                        k++;
                    } while(rs.next());

                    System.out.println("The author named \""+remNumbers(publishersCloseToAuthor)+"\" is mostly associated with these publishers:");
                    int l=1;
                    for (int i=ARRAY_LENGTH-1; i>-1; i--) {
                        String name = publishers.get(i).getName();
                        if (!name.equals("-")) {
                            int lLength = String.valueOf(l).length();
                            System.out.println(l+")"+printChar(" ",3-lLength)+""+publishers.get(i));
                        }
                        if (l==NUMBER_OF_RESULTS) {
                            break;
                        }
                        l++;
                    }
                    System.out.println();
                }
                else {
                    System.out.println("The author does not exist or isn't associated with any publishers.");
                    System.out.println(); 
                }
                findClosestPublishersToAuthor = false;
            }

            //////////////////////////////// Finds the most used keywords by a specific author. ////////////////////////////////////////////////////
            if (findMostUsedKeywordsOfAuthor) {
                ArrayList<KeywordRating> mostUsedKeywords = new ArrayList<>();
                for (int i=0; i<ARRAY_LENGTH;i++) {
                    mostUsedKeywords.add(i,new KeywordRating());
                }

                keywordsOfAuthor = keywordsOfAuthor.toLowerCase();
                rs = selectQuery(conn,"match (a:author)-[:WROTE]-(b) WHERE toLower(a.name) CONTAINS '"+keywordsOfAuthor+"' return id(a),a.name,b.title order by id(a)");
                keywordsOfAuthor = capitalize(keywordsOfAuthor);

                if (rs.next()) {  
                    String prev="-";
                    int k=0;
                    do {
                        String id = rs.getString("id(a)").trim();
                        String name = rs.getString("a.name").trim();
                        String title = rs.getString("b.title").trim();
                        String[] authorKeywords = title.split(" ");

                        if (!prev.equals(id) && k>0) {
                            System.out.println("Most used keywords by author named \""+keywordsOfAuthor+"\":");
                            int l=1;
                            for (int i=ARRAY_LENGTH-1; i>-1; i--) {
                                String keyword = mostUsedKeywords.get(i).getKeyword();
                                if (!keyword.equals("-")) {
                                    int lLength = String.valueOf(l).length();
                                    System.out.println(l+")"+printChar(" ",3-lLength)+""+mostUsedKeywords.get(i));
                                }
                                if (l==NUMBER_OF_RESULTS) {
                                    break;
                                }
                                l++;
                            }
                            System.out.println();
                            System.out.println("There are multiple authors named \""+keywordsOfAuthor+"\".");

                            mostUsedKeywords.clear();
                            for (int i=0; i<ARRAY_LENGTH;i++) {
                                mostUsedKeywords.add(i,new KeywordRating());
                            }
                        }

                        //Changes all the keywords to lower case and singular form. Also removes .,: characters that may be at the end of a keyword.
                        for (int i=0; i<authorKeywords.length; i++) {
                            authorKeywords[i] = authorKeywords[i].replaceAll("[.,:()]", "");
                            authorKeywords[i] = authorKeywords[i].toLowerCase();
                            authorKeywords[i] = inf.singularize(authorKeywords[i]);
                        }

                        for (String keyword : authorKeywords) {
                            //System.out.println(keyword);
                            if (keyword.length()>MIN_KEYWORD_LENGTH-1) {
                                Boolean keywordExists = false;
                                for (int i=0; i<ARRAY_LENGTH; i++) {
                                    if (mostUsedKeywords.get(i).getKeyword().equals(keyword)) {
                                        keywordExists = true;
                                    }
                                }

                                if (!keywordExists) {
                                    ResultSet rs2 = selectQuery(conn,"match (a:author {name: '"+name+"'})-[WROTE]-(b) WHERE toLower(b.title) CONTAINS '"+keyword+"' OR toLower(b.title) CONTAINS '"+inf.pluralize(keyword)+"' return count(b) as timesFound");
                                    if (rs2.next()) {
                                        int timesFound = rs2.getInt("timesFound");
                                        if (timesFound > mostUsedKeywords.get(0).getTimesFound()) {
                                            mostUsedKeywords.set(0,new KeywordRating(keyword,timesFound));
                                            Collections.sort(mostUsedKeywords);  
                                        }
                                    }
                                }
                            }
                        }

                        prev = id;
                        k++;
                    } while (rs.next());

                    System.out.println("Most used keywords by author \""+keywordsOfAuthor+"\":");
                    int l=1;
                    for (int i=ARRAY_LENGTH-1; i>-1; i--) {
                        String keyword = mostUsedKeywords.get(i).getKeyword();
                        if (!keyword.equals("-")) {
                            int lLength = String.valueOf(l).length();
                            System.out.println(l+")"+printChar(" ",3-lLength)+""+mostUsedKeywords.get(i));
                        }
                        if (l==NUMBER_OF_RESULTS) {
                            break;
                        }
                        l++;
                    }
                    System.out.println();
                }
                else {
                    System.out.println("The author does not exist.");
                    System.out.println(); 
                }
                findMostUsedKeywordsOfAuthor = false;
            }

            //////////////////////////////// Finds the most relevant publications to a specific set of keywords. ////////////////////////////////////////////////////
            if (searchBySetOfKeywords) {
                ArrayList<PublicationRating> publications = new ArrayList<>();
                for (int i=0; i<ARRAY_LENGTH;i++) {
                    publications.add(i,new PublicationRating());
                }

                int[] value = new int[keywords.length];
                Boolean[] removeKeyword = new Boolean[keywords.length];

                for (int i=0; i<keywords.length; i++) {
                    //Changes all the keywords to lower case and singular form.
                    keywords[i] = keywords[i].toLowerCase();
                    keywords[i] = inf.singularize(keywords[i]);

                    //Finds the value of a keyword.
                    value[i] = getValueOfKeyword(keywords[i]);
                    //System.out.println("value: "+value[i]);

                    removeKeyword[i] = false;
                    //if enabled removes keywords that are frequently used and wont contribute significantly to the search results.
                    if (removeMostUsedKeywords) {
                        rs = selectQuery(conn,"match (a:keyword) return a.keyword");
                        while (rs.next()) {
                            if (keywords[i].equals(rs.getString("a.keyword"))) {
                                removeKeyword[i] = true;
                            }
                        }
                    }
                }

                for (int i=0; i<keywords.length; i++) {
                    //System.out.println(keywords[i]);
                    if (keywords[i].equals("-")) {
                        break;
                    }
                    
                    if (keywords[i].length()>MIN_KEYWORD_LENGTH-1 && !removeKeyword[i]) {
                        System.out.println(keywords[i]);
                        rs = selectQuery(conn, "match (a)-[:IS]-(b:publication) WHERE toLower(a.title) CONTAINS '"+keywords[i]+"' OR toLower(a.title) CONTAINS '"+inf.pluralize(keywords[i])+"' return a.key,a.title,a.year");
                        while (rs.next()) {
                            String key = rs.getString("a.key").trim();
                            String title = rs.getString("a.title").trim();
                            //System.out.println(title);
                            String year = rs.getString("a.year").trim();
                            //System.out.println(keyword);
                            int rating = 0;
                            for (int j=0; j<keywords.length; j++) {
                                if (keywords[j].equals("-")) {
                                    break;
                                }
                                
                                if (keywords[j].length()>MIN_KEYWORD_LENGTH-1 && !removeKeyword[j]) {
                                    ResultSet rs2 = selectQuery(conn, "match (a {key: '"+key+"' })-[:IS]-(b:publication) WHERE toLower(a.title) CONTAINS '"+keywords[j]+"' OR toLower(a.title) CONTAINS '"+inf.pluralize(keywords[j])+"' return a.key");
                                    if (rs2.next()) {
                                        rating = rating + value[j];
                                    }
                                }
                            }

                            Boolean articleExists = false;
                            for (int j=ARRAY_LENGTH-1; j>-1; j--) {
                                if (publications.get(j).getKey().equals("-")) {
                                    break;
                                }
                                if (publications.get(j).getKey().equals(key)) {
                                    articleExists = true;
                                    break;
                                }
                            }

                            if (!articleExists && rating>publications.get(0).getRating()) {
                                publications.set(0,new PublicationRating(key,title,year,rating));
                                Collections.sort(publications);              
                            }
                        }
                    }
                }

                int l=1;
                if (printMostRelevantArticles) {
                    System.out.println("Most relevant publications to the set of keywords:");
                    for (int i=ARRAY_LENGTH-1; i>-1; i--) {
                        String key = publications.get(i).getKey();

                        if (key.equals("-") && l==1) {
                            System.out.println("No publications were found relevant to these keywords.");
                            break;
                        }
                        if (!key.equals("-")) {
                            int lLength = String.valueOf(l).length();
                            System.out.println(l+")"+printChar(" ",3-lLength)+""+publications.get(i));
                        }
                        if (l==NUMBER_OF_RESULTS) {
                            break;
                        }
                        l++;
                    }
                    System.out.println(); 
                }

                //////////////////// Finds the most relevant journals to a specific set of keywords using the list of the most relevant publications. ///////////////////////////
                if (findBestJournalForArticle) {
                    ArrayList<JournalRating> journals = new ArrayList<>();
                    for (int i=0; i<ARRAY_LENGTH;i++) {
                        journals.add(i,new JournalRating());
                    }

                    for (int i=0; i<ARRAY_LENGTH; i++) {
                        String key = publications.get(i).getKey();
                        if (!key.equals("-")) {
                            //Journals are only associated with articles.
                            rs = selectQuery(conn, "match (a:journal)-[:INCLUDES]-(b:article {key: '"+key+"'}) return a.name,b.year");

                            if (rs.next()) {
                                String name = rs.getString("a.name").trim();
                                int rating = publications.get(i).getRating();

                                Boolean journalExists = false;
                                for (int j=ARRAY_LENGTH-1; j>-1; j--) {
                                    if (journals.get(j).getName().equals("-")) {
                                        break;
                                    }
                                    if (journals.get(j).getName().equals(name)) {
                                        journalExists = true;
                                        journals.get(j).incRating(rating);
                                        break;
                                    }
                                }

                                if (!journalExists) {
                                    rs = selectQuery(conn, "match (a:journal {name: '"+name+"'})-[:INCLUDES]-(n:article) RETURN DISTINCT n.year order by n.year DESC");
                                    if (rs.next()) {
                                        String lastActive = rs.getString("n.year").trim();
                                        //System.out.println(lastActive);
                                        journals.set(0,new JournalRating(name,lastActive,rating)); 
                                    }                    
                                }
                                Collections.sort(journals);
                            } 
                        }
                    }

                    System.out.println("Most relevant journals to the set of keywords:");
                    l=1;
                    for (int i=ARRAY_LENGTH-1; i>-1; i--) {
                        String name = journals.get(i).getName();

                        if (name.equals("-") && l==1) {
                            System.out.println("No journals were found relevant to these keywords.");
                            break;
                        }
                        if (!name.equals("-")) {
                            int lLength = String.valueOf(l).length();
                            System.out.println(l+")"+printChar(" ",3-lLength)+""+journals.get(i));
                        }
                        if (l==NUMBER_OF_RESULTS) {
                            break;
                        }
                        l++;
                    }
                    System.out.println();
                    findBestJournalForArticle = false;
                }

                //////////////////// Finds the most relevant publishers to a specific set of keywords using the list of the most relevant publications. ///////////////////////////
                if (findBestPublisherForPublication) {
                    ArrayList<PublisherRating> publishers = new ArrayList<>();
                    for (int i=0; i<ARRAY_LENGTH;i++) {
                        publishers.add(i,new PublisherRating());
                    }

                    for (int i=0; i<ARRAY_LENGTH; i++) {
                        String key = publications.get(i).getKey();
                        if (!key.equals("-")) {
                            rs = selectQuery(conn, "match (a:publisher)-[:PUBLISHED]-(b {key: '"+key+"'}) return a.name,b.year");

                            if (rs.next()) {
                                String name = rs.getString("a.name").trim();
                                int rating = publications.get(i).getRating();

                                Boolean publisherExists = false;
                                for (int j=ARRAY_LENGTH-1; j>-1; j--) {
                                    if (publishers.get(j).getName().equals("-")) {
                                        break;
                                    }
                                    if (publishers.get(j).getName().equals(name)) {
                                        publisherExists = true;
                                        publishers.get(j).incRating(rating);
                                        break;
                                    }
                                }

                                if (!publisherExists) {
                                    rs = selectQuery(conn, "match (a:publisher {name: '"+name+"'})-[:PUBLISHED]-(n) RETURN DISTINCT n.year order by n.year DESC");
                                    if (rs.next()) {
                                        String lastActive = rs.getString("n.year").trim();
                                        //System.out.println(lastActive);
                                        publishers.set(0,new PublisherRating(name,lastActive,rating)); 
                                    }                    
                                }
                                Collections.sort(publishers);
                            } 
                        }
                    }

                    System.out.println("Most relevant publishers to the set of keywords:");
                    l=1;
                    for (int i=ARRAY_LENGTH-1; i>-1; i--) {
                        String name = publishers.get(i).getName();

                        if (name.equals("-") && l==1) {
                            System.out.println("No publishers were found relevant to these keywords.");
                            break;
                        }
                        if (!name.equals("-")) {
                            int lLength = String.valueOf(l).length();
                            System.out.println(l+")"+printChar(" ",3-lLength)+""+publishers.get(i));
                        }
                        if (l==NUMBER_OF_RESULTS) {
                            break;
                        }
                        l++;
                    }
                    System.out.println();
                    findBestPublisherForPublication = false;
                }

                //////////////// Finds the most relevant authors to a specific set of keywords using the list of the most relevant articles. ////////////////////////////
                if (findBestAuthorForArticle) {
                    ArrayList<AuthorRating> authors = new ArrayList<>();
                    for (int i=0; i<3*ARRAY_LENGTH;i++) {
                        authors.add(i,new AuthorRating());
                    }

                    for (int i=0; i<ARRAY_LENGTH; i++) {
                        String key = publications.get(i).getKey();
                        if (!key.equals("-")) {
                            rs = selectQuery(conn, "match (a:author)-[:WROTE]-(b {key: '"+key+"'}) return a.name,b.year");

                            while (rs.next()) {
                                String name = rs.getString("a.name").trim();
                                int rating = publications.get(i).getRating();


                                Boolean authorsExists = false;
                                for (int j=3*ARRAY_LENGTH-1; j>-1; j--) {
                                    if (authors.get(j).getName().equals("-")) {
                                        break;
                                    }
                                    if (authors.get(j).getName().equals(name)) {   
                                        authorsExists = true;
                                        authors.get(j).incRating(rating);
                                        break;
                                    }
                                }

                                if (!authorsExists) {
                                    ResultSet rs2 = selectQuery(conn, "match (a:author {name: '"+name+"'})-[:WROTE]-(n) RETURN DISTINCT n.year order by n.year DESC");
                                    if (rs2.next()) {
                                       String lastActive = rs2.getString("n.year").trim(); 
                                       //System.out.println(lastActive);
                                        authors.set(0,new AuthorRating(name,lastActive,rating)); 
                                    }               
                                }
                                Collections.sort(authors);
                            }
                        }
                    }

                    System.out.println("Most relevant authors to the set of keywords:");
                    l=1;
                    for (int i=3*ARRAY_LENGTH-1; i>-1; i--) {
                        String name = authors.get(i).getName();

                        if (name.equals("-") && l==1) {
                            System.out.println("No authors were found relevant to these keywords.");
                            break;
                        }
                        if (!name.equals("-")) {
                            int lLength = String.valueOf(l).length();
                            System.out.println(l+")"+printChar(" ",3-lLength)+""+authors.get(i));
                        }
                        if (l==NUMBER_OF_RESULTS) {
                            break;
                        }
                        l++;
                    }
                    System.out.println();
                    findBestAuthorForArticle = false;
                }
                searchBySetOfKeywords = false;
            }
            
            if (!enableUI) {
               conn.close();
               break;
            }
        }
    }
}
