package cis5550.jobs;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.tools.URLParser;
import cis5550.tools.Hasher;
import cis5550.tools.URLEncrypter;

public class PageRank {
	public static void run(FlameContext ctx, String[] args) throws Exception {
		double threshold = Double.valueOf(args[0]);
		double percent = 1;
		if(args.length > 1) {
			percent = Double.valueOf(args[1]) / 100;
		}
		
		FlameRDD rdd = ctx.fromTable("pt-crawl", r -> (r.get("url") + "," + r.get("page")));
		
		FlamePairRDD data = rdd.mapToPair(s -> {
			int firstComma = s.indexOf(",");
			String u = s.substring(0, firstComma);
			String p = s.substring(firstComma + 1);
	
			int indDoc = u.indexOf("#");
			if(indDoc >= 0) {
				u = u.substring(0, indDoc);
			}

			String port = URLParser.parseURL(u)[2];
			if(port == null) {
				String protocol = URLParser.parseURL(u)[0];
				String shost = URLParser.parseURL(u)[1];
				String remain = URLParser.parseURL(u)[3];
				if(protocol.equals("http")) {
					port = "80";
				}else if(protocol.equals("https")){
					port = "443";
				}else {
					System.out.println("Error: only support for http and https");
					return null;
				}
				u = protocol + "://" + shost + ":" + port + remain;
			}
			
			String normalizedList = "";
			List<String> extractedURLs = extractURLs(p);
			Set<String> visited = new HashSet<>();
			String key="cis555";
			for (String e : extractedURLs) {
				String normalized = normalizeURLs(e, u);
				if (normalized != null && !visited.contains(normalized)) {
					visited.add(normalized);
//					String encrpyted = URLEncrypter.encrypt(key, java.net.URLEncoder.encode(normalized, "UTF-8"));
//					normalizedList += (normalizedList.equals("") ? encrpyted : ("," +encrpyted));
					String encoded = Hasher.hash(normalized);
					normalizedList += (normalizedList.equals("") ? encoded : ("," +encoded));
				}
			}
			return new FlamePair( Hasher.hash(u), "1.0,1.0," + normalizedList);
		});
		
		data.saveAsTable("oldState");
		rdd.destroy();
		
		int cnt = 0;
		while(true) {
			//System.out.println("round: " + (++cnt));
			cnt++;
			FlamePairRDD transferedTable = data.flatMapToPair((p) -> {
				List<FlamePair> ans = new ArrayList<>();
				ans.add(new FlamePair(p._1(), 0 + ""));
				int firstComma = p._2().indexOf(",");
				int secondComma = p._2().indexOf(",", firstComma + 1);
				
				double rc = Double.valueOf(p._2().substring(0, firstComma));
				String urls = p._2().substring(secondComma + 1);
				//System.out.println("urls: " + urls);
				if(!urls.equals("")) {
					String urlStrs[] = urls.split(",");
					double decay = 0.85;
					double val = decay * rc / (urlStrs.length);
					for(String url: urlStrs) {
						ans.add(new FlamePair(url, val + ""));
						//System.out.println("url: " + url);
						//System.out.println("val: " + val);
					}
				}
				return ans;
			});
			transferedTable.saveAsTable("trans" + cnt);
			
			
			FlamePairRDD aggragated = transferedTable.foldByKey("0", (a,b) -> ""+(Double.valueOf(a)+Double.valueOf(b)));
			aggragated.saveAsTable("aggragated" + cnt);
			transferedTable.destroy();
//			System.out.println("acheive 94");
			
			FlamePairRDD joined = aggragated.join(data);
			joined.saveAsTable("joined" + cnt);
			aggragated.destroy();
			data.destroy();
			
			data = joined.flatMapToPair(p -> {
//				System.out.println("acheive 96");
				double offset = 0.15;
				List<FlamePair> ans = new ArrayList<>();
				int firstComma = p._2().indexOf(",");
				int secondComma = p._2().indexOf(",", firstComma + 1);
				int thirdComma = p._2().indexOf(",", secondComma + 1);
				Double newVal = Double.valueOf(p._2().substring(0, firstComma)) + offset;
				ans.add(new FlamePair(p._1(), newVal + p._2().substring(firstComma, secondComma) + p._2().substring(thirdComma)));
//				System.out.println("acheive 104");
				return ans;
			});
			
//			System.out.println("acheive 108");
			data.saveAsTable("newState"+ cnt);
			joined.destroy();
			
			FlameRDD diff = data.flatMap(p -> {
				List<String> ans = new ArrayList<>();
				int firstComma = p._2().indexOf(",");
				int secondComma = p._2().indexOf(",", firstComma + 1);
				double dif = Math.abs(Double.valueOf(p._2().substring(0, firstComma)) -
						Double.valueOf(p._2().substring(firstComma + 1, secondComma)));
				ans.add("" + dif);
				return ans;
			});
			diff.saveAsTable("diff"+ cnt);
			
//			String maxDiff = diff.fold("0", (a, b) -> ("" + Math.max(Double.valueOf(a), Double.valueOf(b))));
//			//System.out.println("maxDiff: " + maxDiff);
//			if(Double.valueOf(maxDiff) < threshold) {
//				break;
//			}
			int sum = diff.count();
			FlameRDD achivedNumTable =  diff.flatMap((v) -> {
				double val = Double.valueOf(v);
				List<String> ans = new ArrayList<>();
				if(val < threshold) {
					ans.add("" + 1);
				}
				return ans;
			});
			String achivedNum = achivedNumTable.fold("0", (a, b) -> "" + (Integer.valueOf(a) + Integer.valueOf(b)));
			
			diff.destroy();
			achivedNumTable.destroy();
			System.out.println("cnt: " + cnt + " achived:" + achivedNum + " sum:" + sum + " percent:" + percent);
			if(Double.valueOf(achivedNum) / sum >= percent) {
				break;
			}
			
		}
		
		String kvsAddress = ctx.getKVS().getCoordinator();
		FlamePairRDD pageranks = data.flatMapToPair(p -> {
			int firstComma = p._2().indexOf(",");
			double rank = Double.valueOf(p._2().substring(0, firstComma));
			KVSClient kvs = new KVSClient(kvsAddress);
			//kvs.put("pageranks", p._1(), "rank", rank + "");
			List<FlamePair> ans = new ArrayList<>();
			ans.add(new FlamePair(p._1(), rank + ""));
			return ans;
		});
		
		pageranks.saveAsTable("pt-pageranks");
		
//		KVSClient kvs = new KVSClient(kvsAddress);
//		kvs.rename("pageranks", "pt-pageranks");
		
	}

	public static List<String> extractURLs(String s) {
		List<String> ans = new ArrayList<>();
		int index = 0;
		while (true) {
			index = s.indexOf("<", index);
			// System.out.println("index: " + index);
			if (index < 0) {
				break;
			}
			if (index + 1 < s.length() && s.charAt(index + 1) == '/') {
				index++;
				continue;
			}

			int rightIndex = s.indexOf(">", index + 1);
			if (rightIndex < 0) {
				break;
			}
			String content = s.substring(index + 1, rightIndex);
			String[] strs = content.split(" ");
			for (int i = 1; i < strs.length; i++) {
				String[] strss = strs[i].split("=");
				if (strss.length >= 2 && strss[0].toLowerCase().equals("href") && strss[1].length() >= 2) {
					ans.add(strss[1].substring(1, strss[1].length() - 1));
					// System.out.println("url: " + strss[1].substring(1, strss[1].length() - 1));
				}
			}
			index = rightIndex + 1;
		}
		return ans;
	}

	public static String normalizeURLs(String s, String seedURL) {
		String ans = "";
		int indDoc = s.indexOf("#");
		if (indDoc >= 0) {
			if (indDoc == 0) {
				return null;
			}
			s = s.substring(0, indDoc);
		}

		if (s.indexOf("://") >= 0) {
			String protocol = URLParser.parseURL(s)[0];
			String shost = URLParser.parseURL(s)[1];
			String port = URLParser.parseURL(s)[2];
			String remain = URLParser.parseURL(s)[3];
			if(protocol == null || shost == null || remain == null) {
				return null;
			}
			
			if (port != null) {
				ans = s;
			} else {
				if (protocol.equals("http")) {
					port = "80";
				} else if (protocol.equals("https")) {
					port = "443";
				} else {
					return null;
				}
				ans = protocol + "://" + shost + ":" + port + remain;
			}
		} else if(s.length() == 0) {
			return null;
		}else if(s.charAt(0) != '/') {
			List<String> path = new ArrayList<>();
			String remain = URLParser.parseURL(seedURL)[3];
			String[] strs = remain.split("/");
			for (int i = 1; i < strs.length - 1; i++) {
				path.add(strs[i]);
			}

			String[] strs2 = s.split("/");
			for (int i = 0; i < strs2.length; i++) {
				if (!strs2[i].equals("..")) {
					path.add(strs2[i]);
				} else {
					if(path.size() == 0) {
						return null;
					}
					path.remove(path.size() - 1);
				}
			}

			ans = URLParser.parseURL(seedURL)[0] + "://" + URLParser.parseURL(seedURL)[1] + ":"
					+ URLParser.parseURL(seedURL)[2];
			for (String e : path) {
				ans += "/" + e;
			}
		} else {
			ans = URLParser.parseURL(seedURL)[0] + "://" + URLParser.parseURL(seedURL)[1] + ":"
					+ URLParser.parseURL(seedURL)[2] + s;
		}

		if (ans.endsWith(".jpg") || ans.endsWith(".jpeg") || ans.endsWith(".gif") || ans.endsWith(".png")
				|| ans.endsWith(".txt")) {
			return null;
		} else {
			return ans;
		}
		
	}
}
