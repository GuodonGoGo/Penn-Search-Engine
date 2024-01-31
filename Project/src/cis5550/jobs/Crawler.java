package cis5550.jobs;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.List;
import cis5550.flame.*;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;

public class Crawler {

	public static void run(FlameContext context, String[] args) {
		if (args.length == 0) {
            context.output("Error: Provide a single seed URL as an argument.");
        } else {
        	String url = args[0];
        	long duration = Long.parseLong(args[1])*1000*60;
        	long StartTime = System.currentTimeMillis();
            context.output("Start crawling! Seed url:");
            context.output(url);
            List<String> urlList = new ArrayList<>();
            System.out.println("START");
            try {
//                context.getKVS().delete("pt-visit-history");
//                context.getKVS().delete("pt-crawl");
            	FlameRDD rdd;
            	if (args[2].equals("continue")) {
            		System.out.println("READING QUEUE");
            		rdd = context.fromTable("pt-crawlqueue", row -> {return row.get("value");});
            		System.out.println("READING over");
//            		Iterator<Row> urliter =  context.getKVS().scan("pt-crawlqueue");
//            		while (urliter.hasNext()) {
//            			Row urltocrawl = urliter.next();
//            			urlList.add(urltocrawl.get("value"));
//            		}
//            		System.out.println("READING OVER");
            	}else {
            		urlList.add(url);
            		rdd = (FlameRDDImpl) context.parallelize(urlList);
            	}            	
//            	context.output(rdd.count() + "");
//                String[] seedurl = URLParser.parseURL(url);
//                addhostrecord(context, seedurl);

                while (rdd.count() != 0) {

                    System.out.println("12Current queue length:" + rdd.count() + "");
                    
                    rdd = (FlameRDDImpl) rdd.flatMap(urls -> {
                    	
                    	List<String> results = new ArrayList<>();
                    	if (urls != null && (urls.endsWith(".jpg")
                                || urls.endsWith(".jpeg") || urls.endsWith(".gif")
                                || urls.endsWith(".png") || urls.endsWith(".txt") || urls.endsWith(".ico"))) {
                        	System.out.println("out2");
                            return results;
                        }
                        String[] parsedurl = URLParser.parseURL(urls);
                        String beforepath = addDefaultPortIfNeeded(parsedurl);
                        //If the protocol of the current url is not http/https, then skip 
                        if (beforepath == null) {
                            return results;
                        }
                        
                        String path = parsedurl[3];
                        int lastIndex = parsedurl[3].lastIndexOf("/");
                        if (lastIndex != -1) {
                            path = parsedurl[3].substring(0, lastIndex + 1);
                        }
                                              
                        urls = beforepath+parsedurl[3];
                        
                        
//                        //check visited and allowed
//                        Row visitrecord = context.getKVS().getRow("pt-crawl", Hasher.hash(urls));
//                        Row hostrecord = context.getKVS().getRow("pt-visit-history", Hasher.hash(parsedurl[1]));
//                        
//                        if (visitrecord != null) { 
//                        	System.out.println("out3");
//                        	return results;
//                        }
//                        if (hostrecord == null) {
//                        	System.out.println("addhostrecord"+urls);
//                            addhostrecord(context, parsedurl);
//                        }
//                        
//                        // check allowed list
//                        hostrecord = context.getKVS().getRow("pt-visit-history", Hasher.hash(parsedurl[1]));
//                        String allowed = hostrecord.get("allow");
//                        int isallowed = 1;
//                        for (String i : allowed.split(",")) {
//                            if (i.startsWith("Disallow: ") && Regexcheck(parsedurl[3],i.substring("Disallow: ".length()))){
//                            	isallowed = 0;
//                                break;
//                            }
//                            if (i.startsWith("Allow: ") && Regexcheck(parsedurl[3],i.substring("Disallow: ".length()))){
//                            	isallowed = 1;
//                                break;
//                            }          
//                        }
//                        if (isallowed == 0) {
//                        	System.out.println("out4");
//                            return results;
//                        }
//                        System.out.println("oooooooo"+urls);
                        try {  
                            Row curtimerow = context.getKVS().getRow("pt-visit-history",
                                    Hasher.hash(parsedurl[1]));
                            if (curtimerow == null) {
                            	addhostrecord(context, parsedurl);
                            }
                            long currentTime = System.currentTimeMillis();
                            long timeValue = Long.parseLong(curtimerow.get("time"));
                            double delayValue = Double.parseDouble(curtimerow.get("delay"));
                            // Visiting too often and crawl later
                            if ((currentTime - timeValue) <= (delayValue * 1000)) {
                            	System.out.println("to frequent");
                                results.add(urls);
                                return results;
                            } else {
                                curtimerow.put("time", System.currentTimeMillis() + "");
                            }
                            
                            URL urlObj = new URL(urls);
                            HttpURLConnection headconnection = (HttpURLConnection) urlObj.openConnection();
                            headconnection.setRequestMethod("HEAD");
                            headconnection.setRequestProperty("User-Agent", "CIS550-Crawler");
                            headconnection.setConnectTimeout(3000);
                            headconnection.setReadTimeout(5000);
                            HttpURLConnection.setFollowRedirects(false);
                            headconnection.connect();
                            int headResponseCode = headconnection.getResponseCode();
                            Row r = new Row(Hasher.hash(urls));
                            
                            int texthtmlflag=0;
                            if( headconnection.getContentType()!=null) {
                            	r.put("contentType",headconnection.getContentType());
                            }
                            if( headconnection.getContentLength() != -1) {
                            	r.put("length",""+(headconnection.getContentLength()));
                            }
                            if (headconnection.getContentType()!=null&&headconnection.getContentType().startsWith("text/html")) {
                            	texthtmlflag =1;
                            }
//                            r.put("url", urls);
//                            r.put("responseCode", "" + headResponseCode);
//                            System.out.println("responseCode" + headResponseCode);
                            // Check if the HEAD response code is 200 and content type is acceptable
                            if (headResponseCode == 200) {
                                URL urlOb = new URL(urls);
                                HttpURLConnection connection = (HttpURLConnection) urlOb.openConnection();
                                connection.setRequestMethod("GET");
                                connection.setRequestProperty("User-Agent", "CIS550-Crawler");
                                connection.setConnectTimeout(3000);
                                connection.setReadTimeout(5000);
                                HttpURLConnection.setFollowRedirects(false);
                                connection.connect();
                                
                                
                                int responseCode = connection.getResponseCode();
                                if (responseCode == 200) {
//                                    if( headconnection.getContentType()!=null && !headconnection.getContentType().startsWith("text/html")) {
//                                    	return results;
//                                    }
                                    InputStream inputStream = connection.getInputStream();
                                    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                                    String contentType = connection.getContentType();
                                    int contentLength = connection.getContentLength();

                                    try {
                                        byte[] buffer = new byte[1024]; // 你可以根据需要调整缓冲区大小
                                        int bytesRead;
                                        while ((bytesRead = inputStream.read(buffer)) != -1) {
                                            byteArrayOutputStream.write(buffer, 0, bytesRead);
                                        }
                                    } finally {
                                        inputStream.close();
                                    }
                                    
                                    if (texthtmlflag==1) {
                                        r.put("page", byteArrayOutputStream.toByteArray());
                                        System.out.println("page");
                                    }
//                                    r.put("responseCode", "" + responseCode);
                                    System.out.println("crawled:"+urls);
                                    context.getKVS().putRow("pt-crawl", r);
                                    
                                    byte[] byteArray = byteArrayOutputStream.toByteArray();
                                    String contentAsString = new String(byteArray, StandardCharsets.UTF_8);
                                    results = extracturl(contentAsString);

                                    List<String> updatedResults = new ArrayList<>();
                                    // normalization linked url
                                    for (String rawurl : results) {
//                                    	System.out.println("--------------------------");
//                                    	System.out.println("rawurl!"+rawurl);
                     	                rawurl = normalizeurl(beforepath,path,rawurl);
//	                     	            System.out.println("beforepath!"+beforepath+"----path!"+path+"----rawurl!"+rawurl);
                     	            	if (rawurl!=null&& isLikelyHtmlUrl(rawurl)&& checkallowed(context,rawurl)) {
                     	            	    updatedResults.add(rawurl);
//                     	            	    System.out.println("added!"+rawurl);
                     	            	    
                     	            	}
                     	            	else if(rawurl!=null) {
//                     	            		System.out.println("filtered!"+rawurl);
                     	            	}else{
//                     	            		System.out.println("filternull");
                     	            	}
                     	           	}
                                    
                                    return updatedResults;
                                    
                                } else {
                                    context.getKVS().putRow("pt-crawl", r);
                                    
                                }
                            } else if (headResponseCode == 301 || headResponseCode == 302 || headResponseCode == 303
                                    || headResponseCode == 307 || headResponseCode == 308) {
  
                                context.getKVS().putRow("pt-crawl", r);
                                String redirectUrl = headconnection.getHeaderField("Location");
//                                System.out.println("beforepath"+beforepath);
//                                System.out.println("path"+path);
//                                System.out.println("redirectUrl"+redirectUrl);
                                if(isLikelyHtmlUrl(redirectUrl)) {
                                	 redirectUrl = normalizeurl(beforepath,path,redirectUrl);
                                     if (redirectUrl != null &&isLikelyHtmlUrl(redirectUrl)&& checkallowed(context,redirectUrl)) {
                                     	System.out.println("Redirected to"+redirectUrl);
                                     	results.add(redirectUrl);
                                     }
                                     else {
                                    	 System.out.println("Drop:"+urls);
                                     }
                                }else {
                               	 System.out.println("Drop:"+urls);
                                }
                                return results;
                            } else {
                            	System.out.println("Drop:"+urls);
                                context.getKVS().putRow("pt-crawl", r);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return results;
                    });
                    System.out.println("out6");
                    if(System.currentTimeMillis() - StartTime > duration) {
                    	rdd.saveAsTable("pt-crawlqueue");
                    	context.output("task over!");
                    	break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
	
//	private static List<String> extracturl(String contentAsString) {
//		// TODO Auto-generated method stub
//		return null;
//	}
	public static String addDefaultPortIfNeeded(String[] parsedUrl) {
        // parsedUrl 结构：[协议, 主机, 端口, 路径]
        String protocol = parsedUrl[0];
        String host = parsedUrl[1];
        String port = parsedUrl[2];
        String path = parsedUrl[3];

        // 检查协议是否已知
        if (protocol == null || protocol.isEmpty()||host == null || host.isEmpty() || (!protocol.equalsIgnoreCase("http") && !protocol.equalsIgnoreCase("https"))) {
            return null; // 协议未知，返回空字符串
        }

        // 检查是否已经指定了端口
        if (port == null || port.isEmpty()) {
            if (protocol.equalsIgnoreCase("http")) {
                port = "80";
            } else if (protocol.equalsIgnoreCase("https")) {
                port = "443";
            }
        }

        // 构造完整的 URL 字符串
        String fullUrl = protocol + "://" + host;
        if (port != null && !port.isEmpty()) {
            fullUrl += ":" + port;
        }

        return fullUrl;
    }

	public static String normalizeurl(String beforepath, String path, String rawurl) {
		 if (rawurl==null) {
			 return null;
		 }
		 int fragmentIndex = rawurl.indexOf("#");
         if (fragmentIndex != -1) {
             rawurl = rawurl.substring(0, fragmentIndex);
         }
         if (rawurl.equals("")) {
        	 return null;
         }
         String[] splittedurl = URLParser.parseURL(rawurl);
         if (splittedurl[0] != null && splittedurl[1]!= null) {
        	 if(splittedurl[0].toLowerCase().equals("http") || splittedurl[0].toLowerCase().equals("https")) {
        		 if (splittedurl[2] != null ) {
        			 return rawurl;
        		 }else {
        			 if (splittedurl[0].toLowerCase().equals("http")) {
                    	 rawurl = splittedurl[0] + "://" + splittedurl[1] + ":" + "80"+splittedurl[3];
                    	 return rawurl;
                     } else {
                    	 rawurl = splittedurl[0] + "://" + splittedurl[1] + ":" + "443"+splittedurl[3];
                    	 return rawurl;
                     }
        		 }
        	 } else {
        		 return null;
        	 } 
         }
         //lack of http or hostname
         if (splittedurl[0] != null || splittedurl[1]!= null) {
        	 return null;
         }
         if (rawurl.startsWith("/")) {
             rawurl = beforepath + rawurl;
         } else {
             rawurl = beforepath + path + rawurl;
         }
         while (rawurl.contains("..")) {
        	    int index = rawurl.indexOf("..");

        	    // 确保 '..' 前后有分隔符 '/'
        	    boolean isValid = (index > 0 && rawurl.charAt(index - 1) == '/') &&
        	                      (index + 2 < rawurl.length() && rawurl.charAt(index + 2) == '/');

        	    if (!isValid) {
        	        // 如果 '..' 前后没有 '/', 则跳过这个序列
        	        break;
        	    }

        	    int slashIndex = rawurl.lastIndexOf('/', index - 2);
        	    if (slashIndex != -1) {
        	        rawurl = rawurl.substring(0, slashIndex) + rawurl.substring(index + 3); // 跳过 '..' 和紧随其后的 '/'
        	    } else {
        	        break;
        	    }
        	}
         return rawurl;
		
	}
	
	public static boolean checkallowed(FlameContext context,String normailizedurl) {
		String[] splittedurl = URLParser.parseURL(normailizedurl);
		if (splittedurl[1] == null) {
			return false;
		}
		try {
			Row visitrecord = context.getKVS().getRow("pt-crawl", Hasher.hash(normailizedurl));
			Row hostrecord = context.getKVS().getRow("pt-visit-history", Hasher.hash(splittedurl[1]));
		

			if (visitrecord != null) { 
			  return false;
			 }
			if (hostrecord == null) {
			  System.out.println("addhostrecord");
			  addhostrecord(context, splittedurl);
			  hostrecord = context.getKVS().getRow("pt-visit-history", Hasher.hash(splittedurl[1]));
			}
			String allowed = hostrecord.get("allow");
			
			  int isallowed = 1;
			  for (String i : allowed.split(",")) {
				  if (i.startsWith("Disallow: ") && Regexcheck(splittedurl[3],i.substring("Disallow: ".length()))){
				  	isallowed = 0;
				      break;
				  }
				  if (i.startsWith("Allow: ") && Regexcheck(splittedurl[3],i.substring("Allow: ".length()))){
				      	isallowed = 1;
				          break;
				      }          
				  }
				  if (isallowed == 0) {
					  
				      return false;
				  }
			  return true;
			} catch (Exception e) {
            e.printStackTrace();
            return false;
        }
	}
	
	public static boolean Regexcheck(String path,String rule) {
		String regex = ruleToRegex(rule);
        // 检查路径是否匹配规则
        return Pattern.matches(regex, path);
	}
	
	 private static String ruleToRegex(String rule) {
	        // 转义正则表达式的特殊字符，然后将 '*' 替换为 '.*'
	        // 这将 '*' 转换为正则表达式的通配符
	        String escaped = rule.replaceAll("([\\[\\](){}+.^$|])", "\\\\$1");
	        return "^" + escaped.replaceAll("\\*", ".*") + "$";
	    }
	public static List<String> extracturl(String URL) {
        List<String> extractedUrls = new ArrayList<>();
        // Split the HTML by open angle brackets ("<")
        String[] tags = URL.split("<");

        for (String tag : tags) {
            // Remove any leading and trailing white spaces
            tag = tag.trim();

            // Check if it's not an empty string and starts with "a" (anchor tag)
            if (!tag.isEmpty() && tag.startsWith("a")) {
                // Find the "href" attribute
                int hrefIndex = tag.indexOf("href=\"");

                if (hrefIndex != -1) {
                    int urlStart = hrefIndex + 6; // Start of the URL
                    int urlEnd = tag.indexOf("\"", urlStart); // End of the URL
                    if (urlEnd != -1) {
                        String extractedUrl = tag.substring(urlStart, urlEnd);
                        if (extractedUrl != null && (extractedUrl.endsWith(".jpg")
                                || extractedUrl.endsWith(".jpeg") || extractedUrl.endsWith(".gif")
                                || extractedUrl.endsWith(".png") || extractedUrl.endsWith(".txt") || extractedUrl.endsWith(".ico"))) {
                        }else {
                        	extractedUrls.add(extractedUrl);
                        	if (extractedUrls.size()>= 50) {
                        		break;
                        	}
                        }
                    }
                }
            }
        }
        return extractedUrls;}
//	public static List<String> extracturl(String URL) {
//	    List<String> extractedHtmlUrls = new ArrayList<>();
//	    String[] tags = URL.split("<");
//
//	    for (String tag : tags) {
//	        tag = tag.trim();
//	        if (!tag.isEmpty() && tag.startsWith("a")) {
//	            int hrefIndex = tag.indexOf("href=\"");
//	            if (hrefIndex != -1) {
//	                int urlStart = hrefIndex + 6; // Start of the URL
//	                int urlEnd = tag.indexOf("\"", urlStart); // End of the URL
//	                if (urlEnd != -1) {
//	                    String extractedUrl = tag.substring(urlStart, urlEnd);
//	                    // 使用正则表达式进一步检查 URL
//	                    if (extractedUrl != null) {
//	                        extractedHtmlUrls.add(extractedUrl);
//	                    }
//	                }
//	            }
//	        }
//	    }
//	    return extractedHtmlUrls;
//	}

	// 这个辅助函数使用正则表达式来判断 URL 是否可能是 HTML 文档
	private static boolean isLikelyHtmlUrl(String url) {
	    // 正则表达式匹配不包含明显文件扩展名的 URL
	    return !url.matches(".*\\.(jpg|jpeg|gif|png|txt|ico|pdf|doc|docx|ppt|pptx|xls|xlsx|zip|rar|mp3|mp4)$");
	}

    public static String[] getrobotstxt(String URL) {
        try {
            URL urlOb = new URL(URL + "/robots.txt");
            HttpURLConnection connection = (HttpURLConnection) urlOb.openConnection();
            connection.setRequestMethod("GET");
//            System.out.println("ROBOT CONNECT!");
            connection.setConnectTimeout(3000); // 设置连接超时为3秒
            connection.setReadTimeout(5000);
            connection.connect();
//            System.out.println("ROBOT CONNECT success!");
            int responseCode = connection.getResponseCode();
//            System.out.println("get code success!:"+responseCode);
            if (responseCode == 200) {
//            	System.out.print("ROBOT 200!");
                // 读取 robots.txt 文件内容
                BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                StringBuilder robotstxtContent = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    robotstxtContent.append(line).append("\n");
                }
                reader.close();
//                System.out.println("ROBOT READ OVER!");
                // 解析 robots.txt 内容
                String[] lines = robotstxtContent.toString().split("\n");
                List<String> allowRules = new ArrayList();
                String delay = "1";
                boolean specificRules = false; // 是否有特定的规则适用于 "cis5550-crawler" User-agent
//                System.out.print("ROBOT loop1!");
                for (String robotstxtLine : lines) {
                    String trimmedLine = robotstxtLine.trim();

                    // 检查是否有特定User-agent规则
                    if (trimmedLine.equals("User-agent: cis5550-crawler")) {
                        specificRules = true;
                        continue;
                    } else if (specificRules && trimmedLine.startsWith("Disallow: ")) {
                    	allowRules.add(trimmedLine);
                    } else if (specificRules && trimmedLine.startsWith("Allow: ")) {
                        allowRules.add(trimmedLine);
                    } else if (specificRules && trimmedLine.startsWith("Crawl-delay: ")) {
                        delay = (trimmedLine.substring("Crawl-delay: ".length()));
                    }
                    if(specificRules && trimmedLine.startsWith("User-agent:")){
                    	break;
                    }                   
                }
//                System.out.print("ROBOT loop1 over!");
                if(specificRules == false) {
//                	System.out.print("ROBOT loop2 start!");
                	for (String robotstxtLine : lines) {
                        String trimmedLine = robotstxtLine.trim();
                        // 检查是否有特定User-agent规则
                        if (trimmedLine.equals("User-agent: *")) {
                            specificRules = true;
                            continue;
                        } else if (specificRules && trimmedLine.startsWith("Disallow: ")) {
                        	allowRules.add(trimmedLine);
                        } else if (specificRules && trimmedLine.startsWith("Allow: ")) {
                            allowRules.add(trimmedLine);
                        } else if (specificRules && trimmedLine.startsWith("Crawl-delay: ")) {
                            delay = (trimmedLine.substring("Crawl-delay: ".length()));
                        }
                        if(specificRules && trimmedLine.startsWith("User-agent:")){
                        	break;
                        }
                    }
                	
//                	System.out.print("ROBOT loop2 over!");
                }

                String allowString = "";
                if (!allowRules.isEmpty()) {
                    allowString = String.join(",", allowRules); 
                }
                return new String[] {allowString, delay};
            } else {
                return null; // 没有 robots.txt 文件
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    public static String gethost(String[] seedurl) {
        String host = "";
        if (seedurl[0] == null) {
            System.out.println("Missing host：" + seedurl[3]);
        } else if (!seedurl[0].toLowerCase().equals("http") && !seedurl[0].toLowerCase().equals("https")) {
        	return null;
        } else if (seedurl[2] != null) {
            host = seedurl[0] + "://" + seedurl[1] + ":" + seedurl[2];
        } else {
            if (seedurl[0].toLowerCase().equals("http")) {
                host = seedurl[0] + "://" + seedurl[1] + ":" + "80";
            } else if (seedurl[0].toLowerCase().equals("https")) {
                host = seedurl[0] + "://" + seedurl[1] + ":" + "443";
            }
            else {
            	return null;
            }
        }
        return host;
    }

    public static void addhostrecord(FlameContext context, String[] seedurl) {
        String host = addDefaultPortIfNeeded(seedurl);
        if (host==null) {
        	return;
        }
//        System.out.print("addhostrecord__host:"+host);
        Row visitrow = new Row(Hasher.hash(seedurl[1]));
        String[] rules = getrobotstxt(host);
//        System.out.print("Getrobotover!");
        visitrow.put("time", "" + System.currentTimeMillis());
        if (rules == null) {
            visitrow.put("allow", "");
            visitrow.put("delay", "1");
        } else {
            visitrow.put("allow", rules[0]);
            visitrow.put("delay", rules[1]);
        }
        
        try {
            context.getKVS().putRow("pt-visit-history",visitrow);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
