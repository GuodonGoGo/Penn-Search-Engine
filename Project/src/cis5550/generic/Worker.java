package cis5550.generic;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Random;

public class Worker {
    public static void startPingThread(String workerPort, String coorIpPort, String id) {
        Runnable myRunnable = new Runnable() {
            public void run() {
                while (true) {

                    try {
                        Thread.sleep(5000);
                        String urlStr = "http://" + coorIpPort + "/ping?id=" + id
                                + "&port=" + workerPort;
                        // System.out.println("urlStr: " + urlStr);
                        URL url;
                        url = new URL(urlStr);
                        url.getContent();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        Thread thread = new Thread(myRunnable);
        thread.start();
    }

    public static String generate5RandomLetters() {

        Random r = new Random();
        String ans = "";
        for (int i = 0; i < 5; i++) {
            ans += (char) ('a' + r.nextInt(26));
        }
        return ans;
    }
}
