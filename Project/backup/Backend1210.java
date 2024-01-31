package cis5550.backend;

import static cis5550.webserver.Server.get;
import static cis5550.webserver.Server.port;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import cis5550.external.PorterStemmer;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

public class Backend {
    public static void main(String args[]) throws Exception {
        int portNum = Integer.valueOf(args[0]);
        port(portNum);
        coorPort = args[1];

        int N = Integer.valueOf(args[2]);

        get("/", (req, res) -> {
            return "<html>" + "<h2>HELLO</h2>" + "</html>";
        });

        get("/query", (req, res) -> {

            // get query from request
            String query = req.queryParams("query");

            if (query == null) {
                System.out.println("query is null");
            } else {
                System.out.println("query: " + query);
            }

            List<Map.Entry<String, Boolean>> queryArr = parseQuery(query);

            // stem each term in the query and add into query array
            int arrSize = queryArr.size();
            for (int i = 0; i < arrSize; i++) {
                String q = queryArr.get(i).getKey();
                String stem = getStem(q);
                if (!stem.equals(q)) {
                    queryArr.add(new AbstractMap.SimpleEntry<>(stem, true));
                }
            }

            for (Map.Entry<String, Boolean> t : queryArr) {
                System.out.println(t.getKey());
            }

            // get tf-idf values for each term and put into a "final" list
            Map<String, List<Map.Entry<String, Double>>> TFIDFMap = new HashMap<>();
            for (Map.Entry<String, Boolean> q : queryArr) {

                if (q == null || q.getKey() == null || q.getKey().isEmpty()) {
                    continue;
                }

                System.out.println("hello");

                List<Map.Entry<String, Double>> newTFIDFList = getTFIDF(q.getKey(), N, q.getValue());
                if (newTFIDFList == null || newTFIDFList.size() == 0) {
                    continue;
                }

                System.out.println("bye");

                TFIDFMap.put(q.getKey(), newTFIDFList);

            }

            if (TFIDFMap.size() == 0) {
                return null;
            }

            // combine the scores of tf-idf and pagerank and rank the urls
            List<Map.Entry<String, Double>> rankedList = rankedList(TFIDFMap);

            for (Map.Entry<String, Double> entry : rankedList) {
                System.out.println(entry.getKey() + " " + entry.getValue());
            }

            // create a JSON object array for the ranked URLs
            String jsonArr = toJsonArray(rankedList);

            System.out.println(jsonArr);

            // return the JSON object array
            if (jsonArr != null) {
                res.body(jsonArr);
                res.header("Content-Type", "application/json");
                // res.header("Access-Control-Allow-Origin", "*");
            } else {
                res.status(404, "Not Found");
            }

            return null;

        });

    }

    public static String coorPort;

    public static List<Map.Entry<String, Boolean>> parseQuery(String query) {

        System.out.println("parse query: " + query);

        String[] terms = query.trim().toLowerCase().split("[^a-zA-Z0-9]");

        List<Map.Entry<String, Boolean>> queryList = new ArrayList<>();
        for (String t : terms) {

            System.out.println("!" + t + "!");

            if (t == null || t.length() == 0) {
                continue;
            }

            Map.Entry<String, Boolean> entry = new AbstractMap.SimpleEntry<>(t, false);
            queryList.add(entry);

        }

        return queryList;
    }

    public static List<Map.Entry<String, Double>> getTFIDF(String term, int N, boolean isStem) throws IOException {

        // get tf and idf values from pt-TF and pt-IDF tables
        KVSClient kvs = new KVSClient(coorPort);
        System.out.println("We have " + kvs.numWorkers() + " workers");

        // if term is stemmed, then truncate the value
        double stemFactor = 1;
        if (isStem) {
            stemFactor = 0.7;
        }

        // if either table is empty, the term doesn't exist, hence return null
        Row TFRow = kvs.getRow("pt-TF", term);
        if (TFRow == null) {
            System.out.println("ogTFRow is null");
            return new ArrayList<>();
        }

        // get tf values
        String tfValues = null;
        for (String url : TFRow.columns()) {
            tfValues = TFRow.get(url);
        }

        if (tfValues == null || tfValues.isEmpty()) {
            System.out.println("tfValues is null or empty");
            return new ArrayList<>();
        }

        String[] tfArr = tfValues.split(",");

        // get tf-idf values
        double idf = Math.log(N / tfArr.length);

        // String idf = null;
        // for (String url : IDFRow.columns()) {
        // idf = IDFRow.get(url);
        // }

        if (idf == 0) {
            System.out.println("idf is zero");
            return new ArrayList<>();
        }

        List<Map.Entry<String, Double>> tfidfList = new ArrayList<>();

        System.out.println("URL encode: ");

        for (String urlTF : tfArr) {

            System.out.println(urlTF);

            // get url from urlTF, the format is url:tf
            int i = urlTF.lastIndexOf(":");
            String[] urlTFArr = { urlTF.substring(0, i), urlTF.substring(i + 1) };
            // String url = getRow("pt-crawl", urlTFArr[0].trim()).get("url");
            String url = URLDecoder.decode(urlTFArr[0].trim(), "UTF-8");

            // get tf value
            double tf = Double.parseDouble(urlTFArr[1].trim());

            System.out.println(url);
            System.out.println(Hasher.hash(url));
            System.out.println();

            // calculate tf-idf value
            double TFIDF = tf * idf * stemFactor;

            // create map entry and add to list
            Map.Entry<String, Double> tfidfEntry = new AbstractMap.SimpleEntry<>(url,
                    TFIDF);
            tfidfList.add(tfidfEntry);

        }

        // List<Map.Entry<String, Double>> tfidfList = new ArrayList<>();
        // String idf = IDFRow.get("value");
        // for (String url : TFRow.columns()) {

        // String tf = TFRow.get(url);
        // double tfidf = Double.parseDouble(tf) * Double.parseDouble(idf);

        // Map.Entry<String, Double> tfidfEntry = new AbstractMap.SimpleEntry<>(url,
        // tfidf);
        // tfidfList.add(tfidfEntry);

        // }

        return tfidfList;

    }

    public static List<Map.Entry<String, Double>> rankedList(
            Map<String, List<Map.Entry<String, Double>>> TFIDFMap) throws IOException {

        // combine the score of tf-idf and pagerank and add into a map
        KVSClient kvs = new KVSClient(coorPort);
        Map<String, Double> combinedMap = new TreeMap<>();
        for (String q : TFIDFMap.keySet()) {

            // get list of url + tf-idf entries from arg
            List<Map.Entry<String, Double>> TFIDFList = TFIDFMap.get(q);

            // calculate score for each url
            for (Map.Entry<String, Double> entry : TFIDFList) {

                // get url and tf-idf value
                String url = entry.getKey();
                double TFIDF = entry.getValue();

                // get pagerank value from pt-pageranks table
                double pagerank = 0.0;
                /*
                 * Temporarily comment out when pagranker is unready
                 * // Row pagerankRow = kvs.getRow("pt-pageranks", Hasher.hash(url));
                 * // pagerank = Double.parseDouble(pagerankRow.get("rank"));
                 */

                // combine the two scores and add to list
                double score = (0.7 * TFIDF) + (0.3 * pagerank);

                // add to combined map
                // if the map already contains the url, then add the score to the existing score
                // else create a new entry
                if (combinedMap.containsKey(url)) {
                    double newScore = combinedMap.get(url) + score;
                    combinedMap.put(url, newScore);
                } else {
                    combinedMap.put(url, score);
                }
            }
        }

        // sort the map in descending order
        List<Map.Entry<String, Double>> sortedList = getSortedList(combinedMap);

        // // combine the score of tf-idf and pagerank and add into a list
        // List<Map.Entry<String, Double>> combinedList = new ArrayList<>();
        // for (Map.Entry<String, Double> entry : TFIDFList) {

        // // get url and tfidf value
        // String url = entry.getKey();
        // double tfidf = entry.getValue();

        // // get pagerank value from pt-pageranks table
        // double pagerank = 0.0;
        // Row pagerankRow = getRow("pt-pageranks", Hasher.hash(url));

        // // combine the two scores and add to list
        // double combinedScore = (0.7 * tfidf) + (0.3 * pagerank);

        // Map.Entry<String, Double> combinedEntry = new AbstractMap.SimpleEntry<>(url,
        // combinedScore);
        // combinedList.add(combinedEntry);

        // // limit the search result to at most 50 entries
        // if (combinedList.size() >= 50) {
        // break;
        // }
        // }

        // // sort the list in descending order
        // combinedList.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

        return sortedList;

    }

    public static String getStem(String term) {
        PorterStemmer stemmer = new PorterStemmer();
        stemmer.add(term.toCharArray(), term.length());
        stemmer.stem();
        return stemmer.toString();
    }

    public static SortedSet<Map.Entry<String, Double>> getSortedSet(Map<String, Double> map) {

        // create a sorted set from the map
        SortedSet<Map.Entry<String, Double>> sortedSet = new TreeSet<>(
                (o1, o2) -> o2.getValue().compareTo(o1.getValue()));
        sortedSet.addAll(map.entrySet());

        return sortedSet;

    }

    public static List<Map.Entry<String, Double>> getSortedList(Map<String, Double> map) {

        // create a sorted list from the map
        List<Map.Entry<String, Double>> sortedList = new ArrayList<>(map.entrySet());
        sortedList.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

        return sortedList;

    }

    // public static JSONArray getJSON(SortedSet<Map.Entry<String, Double>>
    // rankedSet) {
    //
    // // create a JSON object array for the ranked URLs
    // JSONArray jsonArray = new JSONArray();
    //
    // for (Map.Entry<String, Double> entry : rankedSet) {
    //
    // String url = entry.getKey();
    // double score = entry.getValue();
    //
    // JSONObject jsonObj = new JSONObject();
    // jsonObj.put("url", url);
    // jsonObj.put("score", score);
    //
    // jsonArray.add(jsonObj);
    //
    // }
    //
    // return jsonArray;
    //
    // }

    public static String toJsonArray(List<Map.Entry<String, Double>> rankedList) {

        StringBuilder sb = new StringBuilder();
        sb.append("[");

        boolean first = true;
        for (Map.Entry<String, Double> entry : rankedList) {

            String url = entry.getKey();

            if (!first) {
                sb.append(",");
            }

            sb.append("{\"url\":\"").append(url).append("\"}");
            first = false;

        }

        sb.append("]");
        return sb.toString();

    }

}
