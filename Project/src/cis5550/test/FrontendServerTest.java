package cis5550.test;

import static cis5550.webserver.Server.get;
import static cis5550.webserver.Server.port;
import static cis5550.webserver.Server.securePort;

import java.net.URLDecoder;
import java.net.URLEncoder;

import cis5550.tools.HTTP;
import cis5550.tools.HTTP.Response;
import cis5550.webserver.Server.staticFiles;

public class FrontendServerTest {

    public static void main(String[] args) {
        // change 8004 for your frontend port
        int frontendPort = Integer.valueOf(args[0]);
        int backendPort = Integer.valueOf(args[1]);

        port(frontendPort);
        securePort(443);

        get("/", (req, res) -> {
            return "Hello World - this is JRYZ";
        });

        staticFiles.location("src/cis5550/frontend");

        get("/query", (req, res) -> {
            String q = req.queryParams("query");
            String[] qs = q.toLowerCase().split("[^a-zA-Z0-9]");
            q = String.join("+", qs);
            // change the 6001 to your backend port
            Response r = HTTP.doRequest("GET", "http://localhost:" + backendPort + "/query?query=" + q, null);
            String s = new String(r.body());

            if (s.length() == 0) {
                s = "[]";
            }
            res.body(s);
            // System.out.println(s + "jaj");
            return null;
        });

        get("/query/:url", (req, res) -> {
            String url = req.params("url");
            url = URLDecoder.decode(url, "UTF-8");
            url = URLEncoder.encode(url, "UTF-8");
            // System.out.println(url);
            Response r = HTTP.doRequest("GET", "http://localhost:" + backendPort + "/query/" + url, null);
            String s = new String(r.body());
            if (s.length() == 0) {
                s = "{}";
            }

            res.body(s);

            return null;
        });
    }

}