package cis5550.kvs;

import static cis5550.webserver.Server.get;
import static cis5550.webserver.Server.port;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

//import org.json.simple.JSONArray;
//import org.json.simple.JSONObject;

import cis5550.tools.Hasher;

public class Coordinator extends cis5550.generic.Coordinator {
    public static void main(String args[]) throws Exception {
        int portNum = Integer.valueOf(args[0]);
        port(portNum);
        registerRoutes();
        get("/", (req, res) -> {
            return "<html>" + "<h2>KVS Coordinator</h2>" + workerTable() + "</html>";
        });
        // securePort(8443);
        // get("/echo/:x", (req,res) -> { return req.params("x"); });
        // get("/session", (req,res) -> { Session s = req.session(); if (s == null)
        // return "null"; return s.id(); });
        // get("/perm/:x", (req,res) -> { Session s = req.session();
        // s.maxActiveInterval(1); if (s.attribute("test") == null) s.attribute("test",
        // req.params("x")); return s.attribute("test"); });

    }

}
