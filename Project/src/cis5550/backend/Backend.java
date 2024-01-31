package cis5550.backend;

import static cis5550.webserver.Server.get;
import static cis5550.webserver.Server.port;
import static cis5550.webserver.Server.securePort;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
                System.out.println("query:\n" + query);
            }

            System.out.println();

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

            System.out.println("queryArr:");
            for (Map.Entry<String, Boolean> t : queryArr) {
                System.out.println(t.getKey());
            }

            System.out.println();

            // get tf-idf values for each term and put into a "final" list
            Map<String, List<Map.Entry<String, Double>>> TFIDFMap = new HashMap<>();
            // List<Map.Entry<String, Double>> queryWeight = new ArrayList<>();
            for (Map.Entry<String, Boolean> q : queryArr) {

                if (q == null || q.getKey() == null || q.getKey().isEmpty()) {
                    continue;
                }

                List<Map.Entry<String, Double>> newTFIDFList = getTFIDF(q.getKey(), N, q.getValue());
                if (newTFIDFList == null || newTFIDFList.size() == 0) {
                    continue;
                }

                // add idf weight to queryWeight list
                // queryWeight.add(newTFIDFList.get(newTFIDFList.size() - 1));

                // add url:tf-idf entries to TFIDFMap
                // TFIDFMap.put(q.getKey(), newTFIDFList.subList(0, newTFIDFList.size() - 1));
                TFIDFMap.put(q.getKey(), newTFIDFList);

            }

            if (TFIDFMap.size() == 0) {
                return null;
            }

            // // get query weight
            // Map<String, Double> queryWeightMap = getQueryWeight(queryWeight);

            // combine the scores of tf-idf and pagerank and rank the urls
            // List<Map.Entry<String, Double>> rankedList = rankedList(TFIDFMap,
            // queryWeightMap);
            List<Map.Entry<String, Double>> rankedList = rankedList(TFIDFMap);

            System.out.println();
            int i = 0;
            for (Map.Entry<String, Double> entry : rankedList) {
                System.out.println(entry.getKey() + " " + entry.getValue());

                i++;
                if (i >= 10) {
                    break;
                }
            }

            System.out.println();

            // create a JSON object array for the ranked URLs
            String jsonArr = toJsonArray(rankedList);

            // System.out.println("jsonArr:\n" + jsonArr);

            // return the JSON object array
            if (jsonArr != null) {
                res.body(jsonArr);
                res.header("Content-Type", "application/json");
                // res.header("Access-Control-Allow-Origin", "*");

                // System.out.println("jsonArr success");
            } else {
                res.status(404, "Not Found");
                // System.out.println("jsonArr NOT success");
            }

            return null;

        });

        get("/query/:url", (req, res) -> {

            String queryURL = req.params("url");

            // System.out.println("encoded url: " + queryURL);

            String url = URLDecoder.decode(queryURL, "UTF-8");

            // System.out.println("decoded url: " + url);

            Map<String, String> pageInfo = getPageInfo(url);
            String jsonDoc = toJson(pageInfo);

            if (jsonDoc != null) {
                res.body(jsonDoc);
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

        String[] terms = query.trim().toLowerCase().split("[^a-zA-Z0-9]");

        List<Map.Entry<String, Boolean>> queryList = new ArrayList<>();
        for (String t : terms) {

            if (t == null || t.length() == 0) {
                continue;
            }

            Map.Entry<String, Boolean> entry = new AbstractMap.SimpleEntry<>(t, false);
            queryList.add(entry);

        }

        return queryList;
    }

    // public static Map<String, Double> getQueryWeight(List<Map.Entry<String,
    // Double>> queryWeight) {

    // Map<String, Double> queryWeightMap = new HashMap<>();

    // queryWeight.sort((o1, o2) -> o1.getValue().compareTo(o2.getValue()));
    // double weight = 1.0;
    // for (Map.Entry<String, Double> entry : queryWeight) {
    // queryWeightMap.put(entry.getKey(), weight);
    // if (weight > 0.7) {
    // weight -= 0.1;
    // }
    // }

    // return queryWeightMap;
    // }

    public static List<Map.Entry<String, Double>> getTFIDF(String term, int N, boolean isStem) {

        try {
            // get tf and idf values from pt-TF and pt-IDF tables
            KVSClient kvs = new KVSClient(coorPort);
            // System.out.println("We have " + kvs.numWorkers() + " workers");

            // if term is stemmed, then truncate the value
            double stemFactor = 1.0;
            if (isStem) {
                // System.out.println("is stem");
                // System.out.println();
                stemFactor = 0.7;
            }

            // if either table is empty, the term doesn't exist, hence return null
            Row TFRow = kvs.getRow("pt-TF", term);
            if (TFRow == null) {
                System.out.println("ogTFRow is null");
                System.out.println();
                return new ArrayList<>();
            }

            // get tf values
            String tfValues = null;
            for (String url : TFRow.columns()) {
                tfValues = TFRow.get(url);
            }

            if (tfValues == null || tfValues.isEmpty()) {
                System.out.println("tfValues is null or empty");
                System.out.println();
                return new ArrayList<>();
            }

            String[] tfArr = tfValues.split(",");

            // get tf-idf values
            // double idf = Math.log(N / tfArr.length);
            // double idf = Math.log10(N / tfArr.length);
            double idf = customLog(500, N / tfArr.length);

            System.out.println(term + " idf value: " + idf);

            // String idf = null;
            // for (String url : IDFRow.columns()) {
            // idf = IDFRow.get(url);
            // }

            if (idf == 0) {
                System.out.println("idf is zero");
                System.out.println();
                return new ArrayList<>();
            }

            List<Map.Entry<String, Double>> tfidfList = new ArrayList<>();

            for (String urlTF : tfArr) {

                // get url from urlTF, the format is url:tf
                int i = urlTF.lastIndexOf(":");
                String[] urlTFArr = { urlTF.substring(0, i), urlTF.substring(i + 1) };
                // String url = getRow("pt-crawl", urlTFArr[0].trim()).get("url");
                String url = URLDecoder.decode(urlTFArr[0].trim(), "UTF-8");

                if (url == null || url.isEmpty() || url.equals("null") || url.contains("\"")
                        || !checkControlChar(url)) {
                    continue;
                }

                // get tf value
                double tf = Double.parseDouble(urlTFArr[1].trim());

                // calculate tf-idf value
                double TFIDF = tf * idf * stemFactor;

                // create map entry and add to list
                Map.Entry<String, Double> tfidfEntry = new AbstractMap.SimpleEntry<>(url,
                        TFIDF);
                tfidfList.add(tfidfEntry);

                if (tfidfList.size() >= 200) {
                    break;
                }
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

            // append the idf value at the end of the list
            // Map.Entry<String, Double> tfidfEntry = new AbstractMap.SimpleEntry<>(term,
            // idf);
            // tfidfList.add(tfidfEntry);

            return tfidfList;

        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    private static boolean checkControlChar(String url) {
        for (int i = 0; i < url.length(); i++) {
            if (url.charAt(i) < 32) {
                return false;
            }
        }
        return true;
    }

    private static double customLog(double base, double logNumber) {
        return Math.log(logNumber) / Math.log(base);
    }

    // public static List<Map.Entry<String, Double>> rankedList(Map<String,
    // List<Map.Entry<String, Double>>> TFIDFMap,
    // Map<String, Double> queryWeightMap) {
    public static List<Map.Entry<String, Double>> rankedList(Map<String, List<Map.Entry<String, Double>>> TFIDFMap) {

        try {
            // combine the score of tf-idf and pagerank and add into a map
            // KVSClient kvs = new KVSClient(coorPort);
            Map<String, Double> combinedMap = new TreeMap<>();
            for (String q : TFIDFMap.keySet()) {

                // get list of url + tf-idf entries from arg
                List<Map.Entry<String, Double>> TFIDFList = TFIDFMap.get(q);
                // // get query weight
                // double queryWeight = queryWeightMap.get(q);

                // calculate score for each url
                for (Map.Entry<String, Double> entry : TFIDFList) {

                    // get url and tf-idf value
                    String url = entry.getKey();
                    double TFIDF = entry.getValue();

                    // get pagerank value from pt-pageranks table
                    // double pagerank = 0.0;
                    /*
                     * Temporarily comment out when pagranker is unready
                     * // Row pagerankRow = kvs.getRow("pt-pageranks", Hasher.hash(url));
                     * // pagerank = Double.parseDouble(pagerankRow.get("rank"));
                     */

                    // combine the two scores and add to list
                    // double score = (0.7 * TFIDF) + (0.3 * pagerank) * queryWeight;
                    double score = (1.0 * TFIDF);

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

            return sortedList.subList(0, Math.min(sortedList.size(), 200));

        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    public static Map<String, String> getPageInfo(String url) {

        // pageInfo will store three values: url, title, abstraction
        Map<String, String> pageInfo = new HashMap<>();

        // store default url, title, and abstraction
        pageInfo.put("url", url);
        pageInfo.put("title", url);
        pageInfo.put("abstract", "No Information Available");

        try {

            // get page of the url
            KVSClient kvs = new KVSClient(coorPort);
            Row urlRow = kvs.getRow("pt-crawl", Hasher.hash(url));
            if (urlRow == null) {
                return pageInfo;
            }

            String page = null;
            if (urlRow.columns().contains("page")) {
                page = urlRow.get("page");
            }

            if (page == null || page.isEmpty()) {
                return pageInfo;
            }

            // get title and abstract
            String title = getTitle(page);
            // String abs = getAbstract(page);

            // System.out.println("abs: " + abs);

            // if abstract is empty or null, we default it to en empty string and add to
            // pageInfo
            // if abstract is not null or not empty, we add it to pageInfo
            // if (abs != null && !abs.isEmpty()) {
            // pageInfo.put("abstract", abs);
            // }

            // if title is empty or null, we default it to the abstract and add to pageInfo
            // if title is empty or null, and abstract is also empty or null, we default it
            // to an url
            // if title is not null or not empty, we add it to pageInfo
            // if (title != null && !title.isEmpty()) {
            // pageInfo.put("title", title);
            // } else {

            // if (abs != null && !abs.isEmpty()) {
            // String newTitle = abs.substring(0, Math.min(abs.length(), 20)) + "...";
            // pageInfo.put("title", newTitle);
            // }

            // }

            if (title != null && !title.isEmpty()) {
                pageInfo.put("abstract", title);
            }

        } catch (Exception e) {
            e.printStackTrace();
            return pageInfo;
        }

        return pageInfo;
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

    public static String getTitle(String page) {

        // create a list of important tags, IN ORDER
        List<String> tags = Arrays.asList("title", "h1", "h2", "h3", "h4", "h5", "h6");

        // loop through the tags and see if there's a match
        for (String tag : tags) {

            // create a pattern and matcher for the tag
            Pattern p = Pattern.compile("<" + tag + "(\\s+[^>]*?)?>(.*?)</" + tag + ">");
            Matcher m = p.matcher(page);

            // if there is a match, we extract it and return it
            if (m.find()) {
                String title = m.group(2);
                String tagsRemoved = title.replaceAll("<.*?>", " ");
                String titleRemoved = tagsRemoved.replaceAll("[\f\b\t\r\n.,:;!?’'\"()-]", " ");
                String backslashRemoved = titleRemoved.replaceAll("\\\\", " ");
                String cntrlChrRemoved = backslashRemoved.replaceAll("[\\p{Cntrl}]", " ");
                String spaceRemoved = cntrlChrRemoved.replaceAll("\\s+", " ");

                // System.out.println("tag: " + tag + " content: " + title);

                return spaceRemoved.trim();
            }
        }

        // if there is no match, we return null
        return null;
    }

    public static String getAbstract(String page) {

        try {
            // check if there are paragraphs in page
            if (!page.contains("<p")) {
                return null;
            }

            // find the p tag in the html
            int tagStart = page.indexOf("<p>");
            // int tagEnd = page.indexOf(" ", tagStart + 3);

            if (tagStart == -1) {
                return null;
            }

            String paragraph = page.substring(tagStart, tagStart + 200).trim();
            String tagsRemoved = paragraph.replaceAll("<.*?>", " ");
            if (tagsRemoved.lastIndexOf("<") != -1) {
                tagsRemoved = tagsRemoved.substring(0, tagsRemoved.lastIndexOf("<"));
            }
            String puncRemoved = tagsRemoved.replaceAll("[\f\b\t\r\n.,:;!?’'\"()-]", " ");
            String backslashRemoved = puncRemoved.replaceAll("\\\\", " ");
            String cntrlChrRemoved = backslashRemoved.replaceAll("[\\p{Cntrl}]", " ");
            String spaceRemoved = cntrlChrRemoved.replaceAll("\\s+", " ");
            return spaceRemoved.trim() + "...";

            // // create a pattern and matcher for a paragraph
            // Pattern p = Pattern.compile("<p[^>]*>(.*?)</p>");
            // Matcher m = p.matcher(page);

            // if (m.find()) {

            // System.out.println("matched");

            // String paragraph = m.group(1);
            // String tagsRemoved = paragraph.replaceAll("<.*?>", " ");
            // String firstSentence = tagsRemoved.split("\\.")[0];

            // return firstSentence;
            // }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
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

    private static String toJson(Map<String, String> data) {

        StringBuilder sb = new StringBuilder();
        sb.append("{");

        boolean first = true;
        for (Map.Entry<String, String> entry : data.entrySet()) {

            if (!first) {
                sb.append(",");
            }

            sb.append("\"").append(entry.getKey()).append("\":\"").append(entry.getValue()).append("\"");
            first = false;
        }

        sb.append("}");
        return sb.toString();
    }
}
