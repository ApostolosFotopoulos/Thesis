package createdatabase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.w3c.dom.*;
import javax.xml.parsers.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import org.xml.sax.SAXException;

public class CreateDatabase {

    public static ResultSet selectQuery(Connection conn,String queryStr) throws SQLException{
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(queryStr);
        return rs;
    }
    
    public static void editQuery(Connection conn,String queryStr) throws SQLException{
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(queryStr);
        }
    }
    
    public static void main(String[] args) throws SQLException {
        String url = "jdbc:neo4j:bolt://localhost";
        Connection conn = null;
        ResultSet rs;
        Inflector inf = new Inflector();
        
        Boolean clearDatabase = true;  //Clears the database before inserting new data.
        
        Boolean insertXml           = true;  //Inserts data from xml files.
        Boolean insertArticles      = true;  //Inserts articles.
        Boolean insertIncollections = true;  //Inserts incollections.
        Boolean insertInproceedings = true;  //Inserts inproceedings.
        Boolean insertBooks         = true;  //Inserts books.
        Boolean insertProceedings   = true;  //Inserts proceedings.
        Boolean insertPhdtheses     = true;  //Inserts phd theses.
        Boolean insertMasterstheses = true;  //Inserts master's theses.
        
        Boolean findMostUsedKeywords   = true;  //Finds the most used keywords by searching the titles that were inserted in the database.
        final int KEYWORD_ARRAY_LENGTH = 200; //The ammount of keywords that will be added in the database.
        final int MIN_KEYWORD_LENGTH   = 5;     //The minimum length of a keyword that will ne added in the database.
                
        try {
            conn = DriverManager.getConnection(url, "neo4j", "1234");
        } catch (SQLException e) {
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }
        
        try {
            System.setProperty("entityExpansionLimit", "2000000");
                
            if (clearDatabase) {
                editQuery(conn,"match (n) optional match (n)-[r]-() delete n,r");
            }
            
            if (insertXml) {
                int k=0;
                File fXmlFile = new File("dblp xmls/dblp"+k+".xml");
                //Insert xmls in the database starting from dblp0.xml (if k equals 0) and continuing with dblp1.xml, dblp2.xml etc.
                while(fXmlFile.exists()) {
                    System.out.println("Inserting "+fXmlFile.getName());
                    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                    Document doc = dBuilder.parse(fXmlFile);
                    doc.getDocumentElement().normalize();
                    
                    //The parent node that all the publications are connected to.
                    editQuery(conn,"create (n:publication {title:'Publication'})");
                    ///////////////////////////////////////////// Inserts articles in the database. //////////////////////////////////////////    
                    if (insertArticles) {
                        NodeList nList = doc.getElementsByTagName("article");
                        for (int temp = 0; temp < nList.getLength(); temp++) {
                            Node nNode = nList.item(temp);

                            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                                Element eElement = (Element) nNode;
                                String key = eElement.getAttribute("key");
                                String title = eElement.getElementsByTagName("title").item(0).getTextContent();
                                String year = eElement.getElementsByTagName("year").item(0).getTextContent();
                                String journal = eElement.getElementsByTagName("journal").item(0).getTextContent();
                                
                                String publisher = "-";
                                if (eElement.getElementsByTagName("publisher").getLength()>0) {
                                    publisher = eElement.getElementsByTagName("publisher").item(0).getTextContent();
                                }
                                
                                String month = "-";
                                if (eElement.getElementsByTagName("month").getLength()>0) {
                                    month = eElement.getElementsByTagName("month").item(0).getTextContent();
                                }
                                
                                String pages = "-";
                                if (eElement.getElementsByTagName("pages").getLength()>0) {
                                    pages = eElement.getElementsByTagName("pages").item(0).getTextContent();
                                }
                                
                                String volume = "-";
                                if (eElement.getElementsByTagName("volume").getLength()>0) {
                                    volume = eElement.getElementsByTagName("volume").item(0).getTextContent();
                                }
                                
                                String number = "-";
                                if (eElement.getElementsByTagName("number").getLength()>0) {
                                    number = eElement.getElementsByTagName("number").item(0).getTextContent();
                                }
                                
                                String ee = "-";
                                if (eElement.getElementsByTagName("ee").getLength()>0) {
                                    ee = eElement.getElementsByTagName("ee").item(0).getTextContent();
                                }
                                
                                title = title.replaceAll("[\\\\'/%$\"]", "");
                                editQuery(conn,"create (n:article {title:'"+title+"', year:'"+year+"', key:'"+key+"'})");
                                editQuery(conn,"match (a:article {key:'"+key+"'}),(b:publication) merge (a)-[r:IS]->(b)");
                                
                                //Inserts the authors of the article in the database.
                                String[] authors = new String[100];                      
                                for (int i=0; i<eElement.getElementsByTagName("author").getLength(); i++) {
                                    authors[i] = eElement.getElementsByTagName("author").item(i).getTextContent();
                                    authors[i] = authors[i].replaceAll("[\\\\'/%$\"]", "");
                                    
                                    rs = selectQuery(conn,"match (n:author {name:'"+authors[i]+"'}) return n.name");                          
                                    if (!rs.next()) {
                                        editQuery(conn,"create (n:author {name:'"+authors[i]+"'})");
                                    }
                                    editQuery(conn,"match (a:author {name:'"+authors[i]+"'}), (b:article {key:'"+key+"'}) merge (a)-[r:WROTE]->(b)");
                                }
                                
                                //Inserts the journal of the article in the database.
                                rs = selectQuery(conn,"match (n:journal {name:'"+journal+"'}) return n.name");                          
                                if (!rs.next()) {
                                    editQuery(conn,"create (n:journal {name:'"+journal+"'})");
                                }
                                editQuery(conn,"match (a:journal {name:'"+journal+"'}), (b:article {key:'"+key+"'}) merge (a)-[r:INCLUDES]->(b)"); 
                                
                                //Inserts the publisher of the article in the database, if the publisher exists.
                                if (!publisher.equals("-")) {
                                    rs = selectQuery(conn,"match (n:publisher {name:'"+publisher+"'}) return n.name");                          
                                    if (!rs.next()) {
                                        editQuery(conn,"create (n:publisher {name: '"+publisher+"'})");
                                    }
                                    editQuery(conn,"match (a:publisher {name:'"+publisher+"'}), (b:article {key:'"+key+"'}) merge (a)-[r:PUBLISHED]->(b)");
                                }
                                
                                //All the properties below are only added if they exist.
                                //Adds the month property to the node.
                                if (!month.equals("-")) {
                                    editQuery(conn,"match (b:article {key:'"+key+"'}) set b.month='"+month+"'");            
                                }
                                
                                //Adds the pages property to the node.
                                if (!pages.equals("-")) {
                                    editQuery(conn,"match (b:article {key:'"+key+"'}) set b.pages='"+pages+"'");            
                                }
                                
                                //Adds the volume property to the node.
                                if (!volume.equals("-")) {
                                    editQuery(conn,"match (b:article {key:'"+key+"'}) set b.volume='"+volume+"'");            
                                }
                                
                                //Adds the number property to the node.
                                if (!number.equals("-")) {
                                    editQuery(conn,"match (b:article {key:'"+key+"'}) set b.number='"+number+"'");            
                                }
                                
                                //Adds the ee (electronic encyclopedia) property to the node.
                                if (!ee.equals("-")) {
                                    editQuery(conn,"match (b:article {key:'"+key+"'}) set b.ee='"+ee+"'");            
                                }
                            }
                        }
                    }
                                                           
                    ///////////////////////////////////////// Inserts incollections in the database. ///////////////////////////////////////////////////////
                    if (insertIncollections) {
                        NodeList nList = doc.getElementsByTagName("incollection");
                        for (int temp = 0; temp < nList.getLength(); temp++) {
                            Node nNode = nList.item(temp);
                            
                            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                                Element eElement = (Element) nNode;
                                String key = eElement.getAttribute("key");
                                String title = eElement.getElementsByTagName("title").item(0).getTextContent();
                                String booktitle = eElement.getElementsByTagName("booktitle").item(0).getTextContent();
                                String year = eElement.getElementsByTagName("year").item(0).getTextContent(); 
                                    
                                String month = "-";
                                if (eElement.getElementsByTagName("month").getLength()>0) {
                                    month = eElement.getElementsByTagName("month").item(0).getTextContent();
                                }
                                
                                String pages = "-";
                                if (eElement.getElementsByTagName("pages").getLength()>0) {
                                    pages = eElement.getElementsByTagName("pages").item(0).getTextContent();
                                }
                                
                                String ee = "-";
                                if (eElement.getElementsByTagName("ee").getLength()>0) {
                                    ee = eElement.getElementsByTagName("ee").item(0).getTextContent();
                                }
                                
                                title = title.replaceAll("[\\\\'/%$\"]", "");
                                booktitle = booktitle.replaceAll("[\\\\'/%$\"]", "");
                                editQuery(conn,"create (n:incollection {title:'"+title+"', booktitle:'"+booktitle+"', year:'"+year+"', key:'"+key+"'})");
                                editQuery(conn,"match (a:incollection {key:'"+key+"'}),(b:publication) merge (a)-[r:IS]->(b)");
                                
                                //Inserts the authors of the incollection in the database.
                                String[] authors = new String[100];                      
                                for (int i=0; i<eElement.getElementsByTagName("author").getLength(); i++) {
                                    authors[i] = eElement.getElementsByTagName("author").item(i).getTextContent();
                                    authors[i] = authors[i].replaceAll("[\\\\'/%$\"]", "");
                                    
                                    rs = selectQuery(conn,"match (n:author {name:'"+authors[i]+"'}) return n.name");                          
                                    if (!rs.next()) {
                                        editQuery(conn,"create (n:author {name:'"+authors[i]+"'})");
                                    }
                                    editQuery(conn,"match (a:author {name:'"+authors[i]+"'}), (b:incollection {key:'"+key+"'}) merge (a)-[r:WROTE]->(b)");
                                }
                                
                                //All the properties below are only added if they exist.
                                //Adds the month property to the node.
                                if (!month.equals("-")) {
                                    editQuery(conn,"match (b:incollection {key:'"+key+"'}) set b.month='"+month+"'");            
                                }
                                
                                //Adds the pages property to the node.
                                if (!pages.equals("-")) {
                                    editQuery(conn,"match (b:incollection {key:'"+key+"'}) set b.pages='"+pages+"'");            
                                }
                                
                                //Adds the ee (electronic encyclopedia) property to the node.
                                if (!ee.equals("-")) {
                                    editQuery(conn,"match (b:incollection {key:'"+key+"'}) set b.ee='"+ee+"'");            
                                }
                            }
                        }
                    }
                    
                    ///////////////////////////////////////// Inserts inproceedings in the database. ///////////////////////////////////////////////////////
                    if (insertInproceedings) {
                        NodeList nList = doc.getElementsByTagName("inproceedings");
                        for (int temp = 0; temp < nList.getLength(); temp++) {
                            Node nNode = nList.item(temp);
                            
                            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                                Element eElement = (Element) nNode;
                                String key = eElement.getAttribute("key");
                                String title = eElement.getElementsByTagName("title").item(0).getTextContent();
                                String booktitle = eElement.getElementsByTagName("booktitle").item(0).getTextContent();
                                String year = eElement.getElementsByTagName("year").item(0).getTextContent(); 
                                    
                                String month = "-";
                                if (eElement.getElementsByTagName("month").getLength()>0) {
                                    month = eElement.getElementsByTagName("month").item(0).getTextContent();
                                }
                                
                                String pages = "-";
                                if (eElement.getElementsByTagName("pages").getLength()>0) {
                                    pages = eElement.getElementsByTagName("pages").item(0).getTextContent();
                                }
                                
                                String ee = "-";
                                if (eElement.getElementsByTagName("ee").getLength()>0) {
                                    ee = eElement.getElementsByTagName("ee").item(0).getTextContent();
                                }
                                
                                title = title.replaceAll("[\\\\'/%$\"]", "");
                                booktitle = booktitle.replaceAll("[\\\\'/%$\"]", "");
                                editQuery(conn,"create (n:inproceedings {title:'"+title+"', booktitle:'"+booktitle+"', year:'"+year+"', key:'"+key+"'})");
                                editQuery(conn,"match (a:inproceedings {key:'"+key+"'}),(b:publication) merge (a)-[r:IS]->(b)");
                                
                                //Inserts the authors of the inproceedings in the database.
                                String[] authors = new String[100];                      
                                for (int i=0; i<eElement.getElementsByTagName("author").getLength(); i++) {
                                    authors[i] = eElement.getElementsByTagName("author").item(i).getTextContent();
                                    authors[i] = authors[i].replaceAll("[\\\\'/%$\"]", "");
                                    
                                    rs = selectQuery(conn,"match (n:author {name:'"+authors[i]+"'}) return n.name");                          
                                    if (!rs.next()) {
                                        editQuery(conn,"create (n:author {name:'"+authors[i]+"'})");
                                    }
                                    editQuery(conn,"match (a:author {name:'"+authors[i]+"'}), (b:inproceedings {key:'"+key+"'}) merge (a)-[r:WROTE]->(b)");
                                }
                                
                                //All the properties below are only added if they exist.
                                //Adds the month property to the node.
                                if (!month.equals("-")) {
                                    editQuery(conn,"match (b:inproceedings {key:'"+key+"'}) set b.month='"+month+"'");            
                                }
                                
                                //Adds the pages property to the node.
                                if (!pages.equals("-")) {
                                    editQuery(conn,"match (b:inproceedings {key:'"+key+"'}) set b.pages='"+pages+"'");            
                                }
                                
                                //Adds the ee (electronic encyclopedia) property to the node.
                                if (!ee.equals("-")) {
                                    editQuery(conn,"match (b:inproceedings {key:'"+key+"'}) set b.ee='"+ee+"'");            
                                }
                            }
                        }
                    }
                    
                    ///////////////////////////////////////// Inserts books in the database. ///////////////////////////////////////////////////////
                    if (insertBooks) {
                        NodeList nList = doc.getElementsByTagName("book");
                        for (int temp = 0; temp < nList.getLength(); temp++) {
                            Node nNode = nList.item(temp);
                            
                            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                                Element eElement = (Element) nNode;
                                String key = eElement.getAttribute("key");
                                String title = eElement.getElementsByTagName("title").item(0).getTextContent();
                                String year = eElement.getElementsByTagName("year").item(0).getTextContent();
                                String publisher = eElement.getElementsByTagName("publisher").item(0).getTextContent();
                                
                                String month = "-";
                                if (eElement.getElementsByTagName("month").getLength()>0) {
                                    month = eElement.getElementsByTagName("month").item(0).getTextContent();
                                }
                                
                                String pages = "-";
                                if (eElement.getElementsByTagName("pages").getLength()>0) {
                                    pages = eElement.getElementsByTagName("pages").item(0).getTextContent();
                                }
                                
                                String series = "-";
                                if (eElement.getElementsByTagName("series").getLength()>0) {
                                    series = eElement.getElementsByTagName("series").item(0).getTextContent();
                                }
                                
                                String volume = "-";
                                if (eElement.getElementsByTagName("volume").getLength()>0) {
                                    volume = eElement.getElementsByTagName("volume").item(0).getTextContent();
                                }
                                
                                String isbn = "-";
                                if (eElement.getElementsByTagName("isbn").getLength()>0) {
                                    isbn = eElement.getElementsByTagName("isbn").item(0).getTextContent();
                                }
                                
                                String ee = "-";
                                if (eElement.getElementsByTagName("ee").getLength()>0) {
                                    ee = eElement.getElementsByTagName("ee").item(0).getTextContent();
                                }
                                
                                title = title.replaceAll("[\\\\'/%$\"]", "");
                                editQuery(conn,"create (n:book {title:'"+title+"', year:'"+year+"', key:'"+key+"'})");
                                editQuery(conn,"match (a:book {key:'"+key+"'}),(b:publication) merge (a)-[r:IS]->(b)");
                                
                                //Inserts the authors of the book in the database.
                                String[] authors = new String[100];                      
                                for (int i=0; i<eElement.getElementsByTagName("editor").getLength(); i++) {
                                    authors[i] = eElement.getElementsByTagName("editor").item(i).getTextContent();
                                    authors[i] = authors[i].replaceAll("[\\\\'/%$\"]", "");
                                    
                                    rs = selectQuery(conn,"match (n:author {name:'"+authors[i]+"'}) return n.name");                          
                                    if (!rs.next()) {
                                        editQuery(conn,"create (n:author {name:'"+authors[i]+"'})");
                                    }
                                    editQuery(conn,"match (a:author {name:'"+authors[i]+"'}), (b:book {key:'"+key+"'}) merge (a)-[r:WROTE]->(b)");
                                }
                                
                                //Insert the publisher of the book in the database.
                                rs = selectQuery(conn,"match (n:publisher {name:'"+publisher+"'}) return n.name");                          
                                if (!rs.next()) {
                                    editQuery(conn,"create (n:publisher {name:'"+publisher+"'})");
                                }
                                editQuery(conn,"match (a:publisher {name:'"+publisher+"'}), (b:book {key:'"+key+"'}) merge (a)-[r:PUBLISHED]->(b)");
                                
                                //All the properties below are only added if they exist.
                                //Adds the month property to the node.
                                if (!month.equals("-")) {
                                    editQuery(conn,"match (b:book {key:'"+key+"'}) set b.month='"+month+"'");            
                                }
                                
                                //Adds the pages property to the node.
                                if (!pages.equals("-")) {
                                    editQuery(conn,"match (b:book {key:'"+key+"'}) set b.month='"+pages+"'");            
                                }
                                
                                //Adds the series property to the node.
                                if (!series.equals("-")) {
                                    editQuery(conn,"match (b:book {key:'"+key+"'}) set b.series='"+series+"'");            
                                }
                                
                                //Adds the volume property to the node.
                                if (!volume.equals("-")) {
                                    editQuery(conn,"match (b:book {key:'"+key+"'}) set b.volume='"+volume+"'");            
                                }
                                
                                //Adds the isbn property to the node.
                                if (!isbn.equals("-")) {
                                    editQuery(conn,"match (b:book {key:'"+key+"'}) set b.isbn='"+isbn+"'");            
                                }
                                
                                //Adds the ee (electronic encyclopedia) property to the node.
                                if (!ee.equals("-")) {
                                    editQuery(conn,"match (b:book {key:'"+key+"'}) set b.ee='"+ee+"'");            
                                }
                            }
                        }
                    }
                    
                    ///////////////////////////////////////// Inserts proceedings in the database. ///////////////////////////////////////////////////////
                    if (insertProceedings) {
                        NodeList nList = doc.getElementsByTagName("proceedings");
                        for (int temp = 0; temp < nList.getLength(); temp++) {
                            Node nNode = nList.item(temp);
                            
                            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                                Element eElement = (Element) nNode;
                                String key = eElement.getAttribute("key");
                                String title = eElement.getElementsByTagName("title").item(0).getTextContent();
                                String year = eElement.getElementsByTagName("year").item(0).getTextContent();
                                String publisher = eElement.getElementsByTagName("publisher").item(0).getTextContent();
                                    
                                String month = "-";
                                if (eElement.getElementsByTagName("month").getLength()>0) {
                                    month = eElement.getElementsByTagName("month").item(0).getTextContent();
                                }
                                
                                String pages = "-";
                                if (eElement.getElementsByTagName("pages").getLength()>0) {
                                    pages = eElement.getElementsByTagName("pages").item(0).getTextContent();
                                }
                                
                                String series = "-";
                                if (eElement.getElementsByTagName("series").getLength()>0) {
                                    series = eElement.getElementsByTagName("series").item(0).getTextContent();
                                }
                                
                                String volume = "-";
                                if (eElement.getElementsByTagName("volume").getLength()>0) {
                                    volume = eElement.getElementsByTagName("volume").item(0).getTextContent();
                                }
                                
                                String isbn = "-";
                                if (eElement.getElementsByTagName("isbn").getLength()>0) {
                                    isbn = eElement.getElementsByTagName("isbn").item(0).getTextContent();
                                }
                                
                                String ee = "-";
                                if (eElement.getElementsByTagName("ee").getLength()>0) {
                                    ee = eElement.getElementsByTagName("ee").item(0).getTextContent();
                                }
                                
                                title = title.replaceAll("[\\\\'/%$\"]", "");
                                editQuery(conn,"create (n:proceedings {title:'"+title+"', year:'"+year+"', key:'"+key+"'})");
                                editQuery(conn,"match (a:proceedings {key:'"+key+"'}),(b:publication) merge (a)-[r:IS]->(b)");
                                
                                //Inserts the authors of the proceedings in the database.
                                String[] authors = new String[100];                      
                                for (int i=0; i<eElement.getElementsByTagName("editor").getLength(); i++) {
                                    authors[i] = eElement.getElementsByTagName("editor").item(i).getTextContent();
                                    authors[i] = authors[i].replaceAll("[\\\\'/%$\"]", "");
                                    
                                    rs = selectQuery(conn,"match (n:author {name:'"+authors[i]+"'}) return n.name");                          
                                    if (!rs.next()) {
                                        editQuery(conn,"create (n:author {name:'"+authors[i]+"'})");
                                    }
                                    editQuery(conn,"match (a:author {name:'"+authors[i]+"'}), (b:proceedings {key:'"+key+"'}) merge (a)-[r:WROTE]->(b)");
                                }
                                
                                //Insert the publisher of the proceedings in the database.
                                rs = selectQuery(conn,"match (n:publisher {name:'"+publisher+"'}) return n.name");                          
                                if (!rs.next()) {
                                    editQuery(conn,"create (n:publisher {name:'"+publisher+"'})");
                                }
                                editQuery(conn,"match (a:publisher {name:'"+publisher+"'}), (b:proceedings {key:'"+key+"'}) merge (a)-[r:PUBLISHED]->(b)");
                                
                                //All the properties below are only added if they exist.
                                //Adds the month property to the node.
                                if (!month.equals("-")) {
                                    editQuery(conn,"match (b:proceedings {key:'"+key+"'}) set b.month='"+month+"'");            
                                }
                                
                                //Adds the pages property to the node.
                                if (!pages.equals("-")) {
                                    editQuery(conn,"match (b:proceedings {key:'"+key+"'}) set b.month='"+pages+"'");            
                                }
                                
                                //Adds the series property to the node.
                                if (!series.equals("-")) {
                                    editQuery(conn,"match (b:proceedings {key:'"+key+"'}) set b.series='"+series+"'");            
                                }
                                
                                //Adds the volume property to the node.
                                if (!volume.equals("-")) {
                                    editQuery(conn,"match (b:proceedings {key:'"+key+"'}) set b.volume='"+volume+"'");            
                                }
                                
                                //Adds the isbn property to the node.
                                if (!isbn.equals("-")) {
                                    editQuery(conn,"match (b:proceedings {key:'"+key+"'}) set b.isbn='"+isbn+"'");            
                                }
                                
                                //Adds the ee (electronic encyclopedia) property to the node.
                                if (!ee.equals("-")) {
                                    editQuery(conn,"match (b:proceedings {key:'"+key+"'}) set b.ee='"+ee+"'");            
                                }
                            }
                        }
                    }
                    
                    if (insertPhdtheses){
                        NodeList nList = doc.getElementsByTagName("phdthesis");
                        for (int temp = 0; temp < nList.getLength(); temp++) {
                            Node nNode = nList.item(temp);
                            
                            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                                Element eElement = (Element) nNode;
                                String key = eElement.getAttribute("key");
                                String title = eElement.getElementsByTagName("title").item(0).getTextContent();
                                String year = eElement.getElementsByTagName("year").item(0).getTextContent();
                                String school = eElement.getElementsByTagName("school").item(0).getTextContent();
                                
                                String publisher = "-";
                                if (eElement.getElementsByTagName("publisher").getLength()>0) {
                                    publisher = eElement.getElementsByTagName("publisher").item(0).getTextContent();
                                }
                                
                                String month = "-";
                                if (eElement.getElementsByTagName("month").getLength()>0) {
                                    month = eElement.getElementsByTagName("month").item(0).getTextContent();
                                }
                                
                                String pages = "-";
                                if (eElement.getElementsByTagName("pages").getLength()>0) {
                                    pages = eElement.getElementsByTagName("pages").item(0).getTextContent();
                                }
                                
                                String series = "-";
                                if (eElement.getElementsByTagName("series").getLength()>0) {
                                    series = eElement.getElementsByTagName("series").item(0).getTextContent();
                                }
                                
                                String volume = "-";
                                if (eElement.getElementsByTagName("volume").getLength()>0) {
                                    volume = eElement.getElementsByTagName("volume").item(0).getTextContent();
                                }
                                
                                String isbn = "-";
                                if (eElement.getElementsByTagName("isbn").getLength()>0) {
                                    isbn = eElement.getElementsByTagName("isbn").item(0).getTextContent();
                                }
                                
                                String ee = "-";
                                if (eElement.getElementsByTagName("ee").getLength()>0) {
                                    ee = eElement.getElementsByTagName("ee").item(0).getTextContent();
                                }
                                
                                title = title.replaceAll("[\\\\'/%$\"]", "");
                                editQuery(conn,"create (n:phdthesis {title:'"+title+"', year:'"+year+"', key:'"+key+"', school:'"+school+"'})");
                                editQuery(conn,"match (a:phdthesis {key:'"+key+"'}),(b:publication) merge (a)-[r:IS]->(b)");
                                
                                //Inserts the authors of the phdthesis in the database.
                                String[] authors = new String[100];                      
                                for (int i=0; i<eElement.getElementsByTagName("author").getLength(); i++) {
                                    authors[i] = eElement.getElementsByTagName("author").item(i).getTextContent();
                                    authors[i] = authors[i].replaceAll("[\\\\'/%$\"]", "");
                                    
                                    rs = selectQuery(conn,"match (n:author {name:'"+authors[i]+"'}) return n.name");                          
                                    if (!rs.next()) {
                                        editQuery(conn,"create (n:author {name:'"+authors[i]+"'})");
                                    }
                                    editQuery(conn,"match (a:author {name:'"+authors[i]+"'}), (b:phdthesis {key:'"+key+"'}) merge (a)-[r:WROTE]->(b)");
                                }
                                
                                //Inserts the publisher of the phdthesis in the database, if the publisher exists.
                                if (!publisher.equals("-")) {
                                    rs = selectQuery(conn,"match (n:publisher {name:'"+publisher+"'}) return n.name");                          
                                    if (!rs.next()) {
                                        editQuery(conn,"create (n:publisher {name: '"+publisher+"'})");
                                    }
                                    editQuery(conn,"match (a:publisher {name:'"+publisher+"'}), (b:phdthesis {key:'"+key+"'}) merge (a)-[r:PUBLISHED]->(b)");
                                }
                                
                                //All the properties below are only added if they exist.
                                //Adds the month property to the node.
                                if (!month.equals("-")) {
                                    editQuery(conn,"match (b:phdthesis {key:'"+key+"'}) set b.month='"+month+"'");            
                                }
                                
                                //Adds the pages property to the node.
                                if (!pages.equals("-")) {
                                    editQuery(conn,"match (b:phdthesis {key:'"+key+"'}) set b.month='"+pages+"'");            
                                }
                                
                                //Adds the series property to the node.
                                if (!series.equals("-")) {
                                    editQuery(conn,"match (b:phdthesis {key:'"+key+"'}) set b.series='"+series+"'");            
                                }
                                
                                //Adds the volume property to the node.
                                if (!volume.equals("-")) {
                                    editQuery(conn,"match (b:phdthesis {key:'"+key+"'}) set b.volume='"+volume+"'");            
                                }
                                
                                //Adds the isbn property to the node.
                                if (!isbn.equals("-")) {
                                    editQuery(conn,"match (b:phdthesis {key:'"+key+"'}) set b.isbn='"+isbn+"'");            
                                }
                                
                                //Adds the ee (electronic encyclopedia) property to the node.
                                if (!ee.equals("-")) {
                                    editQuery(conn,"match (b:phdthesis {key:'"+key+"'}) set b.ee='"+ee+"'");            
                                }
                            }
                        }
                    }
                    
                    if (insertMasterstheses) {
                        NodeList nList = doc.getElementsByTagName("mastersthesis");
                        for (int temp = 0; temp < nList.getLength(); temp++) {
                            Node nNode = nList.item(temp);
                            
                            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                                Element eElement = (Element) nNode;
                                String key = eElement.getAttribute("key");
                                String title = eElement.getElementsByTagName("title").item(0).getTextContent();
                                String year = eElement.getElementsByTagName("year").item(0).getTextContent();
                                String school = eElement.getElementsByTagName("school").item(0).getTextContent();
                                
                                title = title.replaceAll("[\\\\'/%$\"]", "");
                                editQuery(conn,"create (n:mastersthesis {title:'"+title+"', year:'"+year+"', key:'"+key+"', school:'"+school+"'})");
                                editQuery(conn,"match (a:mastersthesis {key:'"+key+"'}),(b:publication) merge (a)-[r:IS]->(b)");
                                
                                //Inserts the authors of the proceedings in the database.
                                String[] authors = new String[100];                      
                                for (int i=0; i<eElement.getElementsByTagName("author").getLength(); i++) {
                                    authors[i] = eElement.getElementsByTagName("author").item(i).getTextContent();
                                    authors[i] = authors[i].replaceAll("[\\\\'/%$\"]", "");
                                    
                                    rs = selectQuery(conn,"match (n:author {name:'"+authors[i]+"'}) return n.name");                          
                                    if (!rs.next()) {
                                        editQuery(conn,"create (n:author {name:'"+authors[i]+"'})");
                                    }
                                    editQuery(conn,"match (a:author {name:'"+authors[i]+"'}), (b:mastersthesis {key:'"+key+"'}) merge (a)-[r:WROTE]->(b)");
                                }
                            }
                        }
                    }

                    k++;
                    fXmlFile = new File("dblp xmls/dblp"+k+".xml");
                }
            }
            
            //Finds the most used keywords and inserts them in the database.
            //The keywords are inserted in their singular form.
            if (findMostUsedKeywords) {
                System.out.println("Finding most used keywords...");
                rs = selectQuery(conn,"match (a)-[:IS]-(b:publication) return a.title");
                
                ArrayList<KeywordRating> mostUsedKeywords = new ArrayList<>();
                for (int i=0; i<KEYWORD_ARRAY_LENGTH;i++) {
                    mostUsedKeywords.add(i,new KeywordRating());
                }
                
                while (rs.next()) {  
                    String title = rs.getString("a.title").trim();
                    String[] keywords = title.split(" ");
                    
                    //Changes all the keywords to lower case and singular form. Also removes .,: characters that may be at the end of a keyword.
                    for (int i=0; i<keywords.length; i++) {
                        keywords[i] = keywords[i].replaceAll("[.,:]", "");
                        keywords[i] = keywords[i].toLowerCase();
                        keywords[i] = inf.singularize(keywords[i]);
                    }
                    
                    for (String keyword : keywords) {
                        //System.out.println(keyword);
                        if (keyword.length()>MIN_KEYWORD_LENGTH-1) {
                            Boolean keywordExists = false;
                            for (int i=0; i<KEYWORD_ARRAY_LENGTH; i++) {
                                if (mostUsedKeywords.get(i).getKeyword().equals(keyword)) {
                                    keywordExists = true;
                                }
                            }
                            
                            if (!keywordExists) {
                                ResultSet rs2 = selectQuery(conn,"match (a)-[:IS]-(b:publication) WHERE toLower(a.title) CONTAINS '"+keyword+"' OR toLower(a.title) CONTAINS '"+inf.pluralize(keyword)+"' return count(a) as timesFound");
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
                }
                
                editQuery(conn,"match (n:keyword) detach delete n");
                for (int i=0; i<KEYWORD_ARRAY_LENGTH; i++) {
                    String keyword = mostUsedKeywords.get(i).getKeyword();
                    if (!keyword.equals("-")) {
                        int timesFound = mostUsedKeywords.get(i).getTimesFound();   
                        editQuery(conn,"create (n:keyword {keyword:'"+keyword+"', timesFound:'"+timesFound+"'})");
                    }
                }
            }
        } catch (IOException | ParserConfigurationException | DOMException | SAXException e) {
            e.printStackTrace(System.out);
        }
        conn.close();
    } 
}
