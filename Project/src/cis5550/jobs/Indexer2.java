package cis5550.jobs;

import java.nio.charset.Charset;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.external.PorterStemmer;
import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.flame.FlameRDDImpl;
import cis5550.flame.Worker;
import cis5550.kvs.KVSClient;
import cis5550.tools.Hasher;

public class Indexer2 {
	public static void run(FlameContext ctx, String[] args) throws Exception {
		FlameRDD rdd = new FlameRDDImpl("pt-pages");
		double N = rdd.count();
		// System.out.println("N: " + N);

//		FlamePairRDD data = rdd.mapToPair(s -> {
//			int firstComma = s.indexOf(",");
//			return new FlamePair(s.substring(0, firstComma), s.substring(firstComma + 1));
//		});
		String kvsAddress = ctx.getKVS().getCoordinator();

		FlamePairRDD wuPairData = rdd.flatMapToPair(s -> {
//			System.out.println("url: " + p._1());
//			System.out.println("first 10 in page: " + p._2().substring(0, 10));
			int firstComma = s.indexOf(",");
			String url = s.substring(0, firstComma);
			String page = s.substring(firstComma + 1);
			List<FlamePair> ans = new ArrayList<>();
			Map<String, Integer> visitedWord = new HashMap<>();
			urlHit(url, visitedWord);
			titleHit(page, visitedWord);

			String tagsRemoved = page.replaceAll("<.*?>", " ");
			String puncRemoved = tagsRemoved.replaceAll("[\t\r\n.,:;!?’'\"()-]", " ");
			String[] words = puncRemoved.trim().split(" +");

			for (int i = 1; i <= words.length; i++) {
				String w = words[i - 1].toLowerCase();
				/* Only support English Word */
				if (!isPureAscii(w) || (!isValidNumber(w) && (!isValidWord(w) || !Worker.lexicon.contains(w)))) {
					continue;
				}
				int occurence = visitedWord.getOrDefault(w, 0);
				visitedWord.put(w, ++occurence);

				PorterStemmer stemmer = new PorterStemmer();
				stemmer.add(w.toCharArray(), w.length());
				stemmer.stem();
				String stemmedW = stemmer.toString();
				if (!stemmedW.equals(w)) {
					int occurence2 = visitedWord.getOrDefault(stemmedW, 0);
					visitedWord.put(stemmedW, ++occurence2);
				}
			}

			for (String w : visitedWord.keySet()) {
				ans.add(new FlamePair(w, url + ":" + visitedWord.get(w)));
				// KVSClient kvs = new KVSClient(kvsAddress);
				// kvs.put("index-3", w, Hasher.hash(p._1()), "" + visitedWord.get(w).size());
			}

			return ans;
		});

		// wuPairData.saveAsTable("pt-index-0");
		// rdd.destroy();

		// System.out.println("acheive 83");
		double coefA = 0.4;
		FlamePairRDD folded = wuPairData.foldByKey("", (a, b) -> (a.equals("") ? b : a + "," + b));
		// wuPairData.saveAsTable("folded");
		// wuPairData.destroy();
		// folded.saveAsTable("pt-folded");

		FlamePairRDD index = folded.flatMapToPair(p -> {

			// System.out.println("acheive 88");
			// File Name Length Limit
			if (p._1().length() > 100) {
				return new ArrayList<>();
			}

			// System.out.println("acheive 94");
			List<String> urls = Arrays.asList(p._2().split(","));
			Collections.sort(urls, (a, b) -> {
				int lastIndex1 = a.lastIndexOf(":");
				int lastIndex2 = b.lastIndexOf(":");
				int num1 = Integer.valueOf(a.substring(lastIndex1 + 1));
				int num2 = Integer.valueOf(b.substring(lastIndex2 + 1));
				return num2 - num1;
			});
			int lastIndex = urls.get(0).lastIndexOf(":");
			int maxFreq = Integer.valueOf(urls.get(0).substring(lastIndex + 1));

			List<String> standardizedUrls = new ArrayList<>();
			for (String url : urls) {
				int lastInd = url.lastIndexOf(":");
				int num = Integer.valueOf(url.substring(lastInd + 1));
				double val = coefA + (1 - coefA) * num / maxFreq;
				standardizedUrls.add(url.substring(0, lastInd) + ":" + val);
			}

			String sortedUrls = String.join(",", standardizedUrls);
			List<FlamePair> ans = new ArrayList<>();
			ans.add(new FlamePair(p._1(), sortedUrls));
			// kvs.put("pt-index", p._1(), "urls", sortedUrls);
			return ans;
		});

		// index.saveAsTable("pt-TF");
		// folded.destroy();

//		FlamePairRDD IDF = index.flatMapToPair(p -> {
//			
//			//System.out.println("acheive 88");
//			// File Name Length Limit
//			if(p._1().length() > 100) {
//				return new ArrayList<>();
//			}
//			
//			//System.out.println("acheive 94");
//			List<String> urls = Arrays.asList(p._2().split(","));
//			double idf =  Math.log(N / urls.size());
//			
//			/* Could return a RDD, and store it to disk to reduce I/O cost */
//			//kvs.put("pt-IDF", p._1(), "value", "" + idf);
//			List<FlamePair> ans = new ArrayList<>();
//			ans.add(new FlamePair(p._1(), idf + ""));
//			
//			
//			return ans;
//		});

		// IDF.saveAsTable("pt-IDF");

		System.out.println(LocalDateTime.now());
		// KVSClient kvs = new KVSClient(kvsAddress);
		// kvs.rename("index", "pt-index");
		// kvs.rename("IDF", "pt-IDF");
		// kvs.rename("TF", "pt-TF");

//		KVSClient kvs = ctx.getKVS();
//		kvs.rename("pt-crawl", "pt-crawl-old");
	}

	private static void titleHit(String page, Map<String, Integer> visitedWord) {
		// TODO Auto-generated method stub
		Map<String, Integer> map = new HashMap<>();
		map.put("title", 50);
		map.put("h1", 30);
		map.put("h2", 20);
		map.put("h3", 10);
		map.put("h4", 5);
		map.put("h5", 3);
		
		for(String tag: map.keySet()) {
			int tagVal = map.get(tag);
			Pattern p = Pattern.compile("<" + tag + ">(.*?)</" + tag + ">");
			Matcher m = p.matcher(page);
			while (m.find() == true) {
				String title = m.group(1);
				String tagsRemoved = title.replaceAll("<.*?>", " ");
				String titleRemoved = tagsRemoved.replaceAll("[\t\r\n.,:;!?’'\"()-]", " ");
				String[] words = titleRemoved.trim().split(" +");
				for (String uw : words) {
					String w = uw.toLowerCase();
					if (w.equals("http") || w.equals("https") || w.equals("")) {
						continue;
					}
					if (isPureAscii(w) && isValidWord(w) && Worker.lexicon.contains(w)) {
						int occurence = visitedWord.getOrDefault(w, 0);
						visitedWord.put(w, occurence + tagVal);
						
						PorterStemmer stemmer = new PorterStemmer();
						stemmer.add(w.toCharArray(), w.length());
						stemmer.stem();
						String stemmedW = stemmer.toString();
						if (!stemmedW.equals(w)) {
							int occurence2 = visitedWord.getOrDefault(stemmedW, 0);
							visitedWord.put(stemmedW, occurence + tagVal);
						}
					}

				}
			}
		}
		

	}

	private static void urlHit(String url, Map<String, Integer> visitedWord) {
		String urlRemoved = url.replaceAll("[\t\r\n.,:;!?’'\"()-]", " ");
		String[] words = urlRemoved.trim().split(" +");
		for (String uw : words) {
			String w = uw.toLowerCase();
			if (w.equals("http") || w.equals("https")|| w.equals("")) {
				continue;
			}
			if (isPureAscii(w) && isValidWord(w) && Worker.lexicon.contains(w)) {
				int occurence = visitedWord.getOrDefault(w, 0);
				visitedWord.put(w, occurence + 30);
				
				PorterStemmer stemmer = new PorterStemmer();
				stemmer.add(w.toCharArray(), w.length());
				stemmer.stem();
				String stemmedW = stemmer.toString();
				if (!stemmedW.equals(w)) {
					int occurence2 = visitedWord.getOrDefault(stemmedW, 0);
					visitedWord.put(stemmedW, occurence + 30);
				}
			}
			
			

		}

	}

	public static boolean isPureAscii(String v) {
		return Charset.forName("US-ASCII").newEncoder().canEncode(v);
	}

	public static boolean isValidWord(String w) {
		for (int i = 0; i < w.length(); i++) {
			char c = w.charAt(i);
			if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'))) {
				return false;
			}
		}
		return true;
	}

	public static boolean isValidNumber(String w) {
		if (w.length() > 3) {
			return false;
		}

		for (int i = 0; i < w.length(); i++) {
			char c = w.charAt(i);
			if (!(c >= '0' && c <= '9')) {
				return false;
			}
		}
		return true;
	}

}
