/*********************************
 * CG3204L - Assignment 2
 * Simple Web Crawler
 * Ken Tran - U099095A
 * ******************************/

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.io.*;
import java.net.*;
import java.sql.*;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class WebCrawler {
    private static int maxThreadNo = 3;
    private static Set<String> storage;
    private static Queue<String> frontier;
   
    public static void main(String[] args) throws Exception {
        
        System.setProperty("sun.net.client.defaultConnectTimeout", "500");
        System.setProperty("sun.net.client.defaultReadTimeout", "1000");

        // Initial web page
        String url1 = "www.nus.edu.sg";
        String url2 = "cnn.com";
        String url3 = "techcrunch.com";

        // List of web page to be examined
        frontier = new LinkedList<String>();
        frontier.offer(url1);
        frontier.offer(url2);
        frontier.offer(url3);

        // Set of examined web pages
        storage = new HashSet<String>();
        storage.add(url1);
        storage.add(url2);
        storage.add(url3);

        /* Start the thread */
        for (int i = 0; i < maxThreadNo; i++) {
            Thread thread = new Thread(new CrawlerProcess(frontier, storage, i));
            thread.start();
            Thread.currentThread().sleep(1000);
        }
    }
}

final class CrawlerProcess implements Runnable {
    private static Socket s;
    private static Queue<String> frontier;
    private static Set<String> storage;
    private static BufferedWriter bw;

    private static BufferedReader is;
    private static DataOutputStream os;

    private static String hostname;
    private static String html;

    private static int name;

    private static String dbconnect = "jdbc:mysql://127.0.0.1/crawltest";
    private static Connection c;

    private long elapsedTime = 0;

    CrawlerProcess (Queue<String> frontier, Set<String> storage, int name) {
        this.frontier = frontier;
        this.storage = storage;
        this.name = name;
    }

    public void run() {
        try {
            File file = new File(name + ".txt");
            if(!file.exists())
                file.createNewFile();
            BufferedWriter bw = new BufferedWriter(new FileWriter(file.getAbsoluteFile()));
            int i = 0; 

            connectDB();
        
        while (true) {
            if (isDone())
                break;

            /* Get the next hostname from the frontier */
            hostname = getNextHost();
            
            try {
                /* Create a socket to the hostname at port 80 */
                s = new Socket(hostname, 80);

                /* Create input and out stream */
                is = new BufferedReader(new InputStreamReader(s.getInputStream()));
                os = new DataOutputStream(s.getOutputStream());

                long startTime = System.nanoTime();

                /* Send a request to the server */
                os.write("GET / HTTP/1.0\r\n\r\n".getBytes());

                //long stopTime = System.nanoTime();
                //elapsedTime = (stopTime - startTime)/1000;

                StringBuilder input = new StringBuilder(); // store the whole response from the server
                String line = is.readLine();

                while (line != null) {
                    input.append(line);
                    line = is.readLine();
                }
                long stopTime = System.nanoTime();
                elapsedTime = (stopTime - startTime)/1000;
                html = input.toString();
            } catch (Exception e) {
                System.out.println(e + " when trying to get the response from server");
            }

            /* Parse the html text to Jsoup Document type */
            Document doc = Jsoup.parse(html);

            /* Get all links in "href" */
            Elements a_elems = doc.select("a[href]");
            for (Element a: a_elems) {
                String link = a.attr("abs:href").toString();
                String newHostname = getHostname(link);
                
                /* Put the links into the frontier and storage if applicable */
                putNewHost(newHostname);
            }
            try {
                /* When done, close input, output stream and the socket */
                is.close();
                os.close();
                s.close();
                Thread.currentThread().sleep(3000);
            } catch (Exception e) {
                System.out.println(e + " when close the socket");
            }
            
            /* Output the result on the terminal screen
             * and write to files
             */
            System.out.println(hostname + " - Elapsed Time: " + elapsedTime + "us");
            try {
                bw.write(hostname + " - Elapsed Time: " + elapsedTime + "us\n");
            } catch (IOException e) {
                System.out.println(e + " when write to files");
            }

            try {
                //useMySQL(hostname, elapsedTime);
            } catch (Exception e) {
                System.out.println(e);
            }

            /* Set the limit for each thread here if necessary */
            i++;
            if (i==5)
                break;
        }

            /* Close the file when done */ 
            bw.close();
            
        } catch(IOException e) {
            System.out.println(e + " when handle files");
            System.exit(0);
        }
        System.out.println("Done");
    }
   
    /* Make sure all threads are synchronized when they
     * access the storage or frontier
     */
    private synchronized String getNextHost () {
        synchronized (frontier) {
            return frontier.poll();
        }
    }

    private synchronized void putNewHost (String newHostname) {
        if (isValid(newHostname)) {
            synchronized (frontier) {
                frontier.offer(newHostname);
            }
            synchronized (storage) {
                storage.add(newHostname);
            }
        }
    }

    private synchronized boolean isValid (String newHostname) {
        if (storage.contains(newHostname) || storage.contains("www." + newHostname)
                || newHostname.equals(""))
            return false;
        else 
            return true;
    }

    private synchronized boolean isDone() {
        return frontier.isEmpty();
    }

    /* Only get the hostname 
     * for example: http://xxx.yyy.zzz/abc/edf
     * will become xxx.yyy.zzz
     */
    private String getHostname (String link) {
        int doubleSlash = link.indexOf("//");

        if (doubleSlash == -1) 
            doubleSlash = 0;
        else
            doubleSlash += 2;

        int end = link.indexOf("/", doubleSlash);
        end = (end >= 0) ? end : link.length();
        
        return link.substring(doubleSlash, end);
    }

    /* Connect to DB */
    private void connectDB() 
    {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            c = DriverManager.getConnection(dbconnect, "root", "excalibur");
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private void useMySQL(String Hostname, long time) throws SQLException
    {
        PreparedStatement pst = c.prepareStatement("Select count(*) from result where name = ?");
        pst.setString(1, Hostname);
        ResultSet rs = pst.executeQuery();
        rs.next();
        if (rs.getInt(1) > 0) {
            //System.out.println("Host existed in database!");
            return;
        }

        pst = c.prepareStatement("Insert into result(name, time) values(?, ?)");
        pst.setString(1, Hostname);
        pst.setLong(2, time);

        pst.executeUpdate();
    }
}
