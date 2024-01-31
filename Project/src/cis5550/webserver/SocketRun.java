package cis5550.webserver;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.file.Files;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import cis5550.tools.Logger;

import java.net.*;

public class SocketRun implements Runnable{
	private static final Logger logger = Logger.getLogger(SocketRun.class);
	Socket sock;
	String fileName;
	Server server;
	SocketRun(Socket sock, String fileName, Server server) {
        this.sock = sock;
        this.fileName = fileName;
        this.server = server;
    }
	@Override
	public void run(){
		
		while(true) {
			// Each iteration handle one HTTP request
			boolean isFail = false;
			boolean isRouteExist = false;
	    	String output = "";
		    ByteArrayOutputStream baos = new ByteArrayOutputStream();
		    ByteArrayOutputStream bodyBaos = new ByteArrayOutputStream();
		    
		    Map<String,String> headers = new HashMap<>();
		    
		    byte[] buf = new byte[1024];
		    int n = 0;
		    int readedBody = 0;
		    while(true){
		    	//System.out.println("achieve 45");
		    	try {
					n = sock.getInputStream().read(buf);
				} catch (IOException e) {
					e.printStackTrace();
				}
		    	//System.out.println("n: " + n);
		    	
		    	if(n <= 0) {
		    		break;
		    	}
		    	
		    	int endedInd = checkDoubleCRLF(buf, n);
		    	if(endedInd == n) {
		    		baos.write(buf, 0, n); 
		    	}else {
		    		// Achieve the end of this request when encountering double CRLF
		    		baos.write(buf, 0, endedInd);
		    		bodyBaos.write(buf, endedInd + 2, n - (endedInd + 2));
		    		readedBody += n - (endedInd + 2);
		    		break;
		    	}
		    };
		
		    // No more requests on this connection
		    if(n <= 0) {
		    	try {
					baos.close();
					bodyBaos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		    	break;
		    }
		    
		    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray() );
		    BufferedReader bfReader = new BufferedReader(new InputStreamReader(bais));
	        
		    String contentLength = null;
		    String host = null;
		    String ims = null;
		    String contentType = null;
	
		    String firstLineRes = "";
		    String firstLine = "";
			try {
				firstLine = bfReader.readLine();
				firstLineRes = handleFirstLine(firstLine);
			} catch (IOException e) {
				e.printStackTrace();
			}
			//System.out.println("firstLine: " + firstLine);
		    if(!firstLineRes.equals("Currently OK")) {
		    	output += firstLineRes;
		    	isFail = true;
		    }
		    
	    	String line = null;
	    	try {
				while((line = bfReader.readLine()) != null){
				    if(line.toLowerCase().startsWith("content-length: ")) {
				    	contentLength = line.toLowerCase().substring("content-length: ".length());
				    	headers.put("content-length", contentLength);
				    	//System.out.println("contentLength: " + contentLength);
				    }else if(line.toLowerCase().startsWith("host: ")) {
				    	host = line.toLowerCase().substring("host: ".length());
						if(host.indexOf(":") >= 0) {
							host = host.substring(0, host.indexOf(":"));
						}
				    	
				    	headers.put("host", host);
				    }else if(line.toLowerCase().startsWith("if-modified-since: ")) {
				    	ims = line.toLowerCase().substring("if-modified-since: ".length());
				    	headers.put("if-modified-since", ims);
				    }else if(line.toLowerCase().startsWith("content-type: ")) {
				    	contentType = line.toLowerCase().substring("content-type: ".length());
				    	headers.put("content-type", contentType);
				    }else if(line.toLowerCase().indexOf(": ") >= 0){
				    	int ind = line.toLowerCase().indexOf(": ");
				    	headers.put(line.toLowerCase().substring(0, ind), line.substring(ind + 2));
				    }
					//System.out.println(line);
				}
				try {
					baos.close();
					bais.close();
					bfReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
	    	
	    	if(contentLength != null) {
	    		if(!contentLength.equals("0")) {
	    			int count = Integer.valueOf(contentLength);
	    			//System.out.println("count: " + count);
	    			// Continue to fetch content in request body if the content-length hasn't been achieved
		    		while(readedBody < count){
				    	try {
							n = sock.getInputStream().read(buf);
							bodyBaos.write(buf, 0, n);
					    	readedBody += n; 
						} catch (IOException e) {
							e.printStackTrace();
						}
				    }
		    		
		    		try {
						bodyBaos.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
	    		}
	    	}
	    	
	    	
	    	File file = null;
	    	if(!isFail) {
	    		String[] strs =  firstLine.split(" ");
	    		Map<String, Route> routesMap = null;
	    		if(strs[0].equals("GET")) {
	    			if(Server.hostGetTable.containsKey(host)) {
	    				routesMap = Server.hostGetTable.get(host);
	    			}else {
	    				routesMap = Server.getTable;
	    			}
	    		}else if(strs[0].equals("POST")) {
	    			if(Server.hostPostTable.containsKey(host)) {
	    				routesMap = Server.hostPostTable.get(host);
	    			}else {
	    				routesMap = Server.postTable;
	    			}
	    		}else if(strs[0].equals("PUT")) {
	    			if(Server.hostPutTable.containsKey(host)) {
	    				routesMap = Server.hostPutTable.get(host);
	    			}else {
	    				routesMap = Server.putTable;
	    			}
	    		}
	    		
	    		int indOfQmark = strs[1].indexOf("?");
	    		String path = "", queryPart = "";
	    		if(indOfQmark >= 0) {
	    			path = strs[1].substring(0, indOfQmark);
	    			queryPart = strs[1].substring(indOfQmark + 1);
	    		}else {
	    			path = strs[1];
	    			queryPart = "";
	    		}
	    		
	    		String matchedPath = pathMatch(routesMap, path);
	    		
	    		if(routesMap.containsKey(path) || !matchedPath.equals("")) {
	    			isRouteExist = true;
	    			Route route = routesMap.get(path);
	    			//System.out.println("Byte Array Len: " + bodyBaos.toByteArray().length);
	    			Map<String,String> queryParams = new HashMap<>();
	    			Map<String,String> argParams = new HashMap<>();
	    			
	    			if(!matchedPath.equals("")) {
	    				findArgParms(matchedPath, path, argParams);
	    				route = routesMap.get(matchedPath);
	    			}
	    			
	    			findqueryParms(queryPart, queryParams);
	    			
	    			if(contentType != null && contentType.equals("application/x-www-form-urlencoded")) {
	    				findqueryParms(bodyBaos.toString(), queryParams);
	    			}
	    			
	    			RequestImpl req = new RequestImpl(strs[0], path, strs[2], headers, queryParams, argParams, (InetSocketAddress)sock.getRemoteSocketAddress(), bodyBaos.toByteArray(), server);
	    			ResponseImpl res = new ResponseImpl(sock);
	    			
	    			Object resBody = null;
	    			boolean isException = false;
	    			try {
	    				if(Server.routeBefore != null) {
	    					Server.routeBefore.handle(req, res);
	    				}
	    				if(res.statusCode != 200) {
	    					isException = true;
	    					res.header("content-length", "0");
	    				}else {
	    					res.stage = 1;
		    				resBody = route.handle(req, res);
		    				res.stage = 2;
		    				if(Server.routeAfter != null) {
		    					Server.routeAfter.handle(req, res);
		    				}
	    				}
					} catch (Exception e) {
						e.printStackTrace();
						isException = true;
						if(!res.isWriteBefore) {
							res.status(500, "Internal Server Error");
							res.header("content-length", "0");
						}
						
					}
	    			
	    			if(!res.isWriteBefore) {
	    				if(!isException) {
	    					if(resBody != null) {
	    						res.header("content-length", "" + resBody.toString().getBytes().length);
	    					}else if(res.recentBodyCall == 1) {
//	    						System.out.println("Body in server: " +  res.bodyRaw);
//	    						System.out.println("Body String in server: " +  new String(res.bodyRaw));
//	    						System.out.println("lenth: " + res.bodyRaw.length);
	    						res.header("content-length", "" + res.bodyRaw.length);
	    					}else if(res.recentBodyCall == 2) {
	    						res.header("content-length", "" + res.body.getBytes().length);
	    					}else {
	    						res.header("content-length", "0");
	    					}
	    				}
	    				if(res.headers.get("content-type") == null) {
	    					res.header("content-type", "text/html");
	    				}
	    				output += "HTTP/1.1 " + res.statusCode + " " + res.reasonPhrase + "\r\n";
		    			for(String key: res.headers.keySet()) {
		    				output += key + ": " + res.headers.get(key) + "\r\n";
		    			}
		    			output += "\r\n";
		    			if(!isException) {
		    				if(resBody != null) {
		    					output += resBody.toString();
		    				}else if(res.recentBodyCall == 1) {
		    					output += new String(res.bodyRaw);
		    				}else if(res.recentBodyCall == 2) {
		    					output += res.body;
		    				}
		    			}
		    			
		    			//System.out.println("output: " + output);
		    			PrintWriter out;
						try {
							out = new PrintWriter(sock.getOutputStream());
							out.print(output);
						    out.flush();
						} catch (IOException e) {
							e.printStackTrace();
						}
	    			}else {
	    				break;
	    			}
	    		}else {
	    			String fileURL = fileName + "/" + strs[1];
			    	file = new File(fileURL);
			  
			    	if(!strs[0].equals("GET") && !strs[0].equals("HEAD") ) {
			        	output += "HTTP/1.1 405 Not Allowed\r\n";
			        	isFail = true;
			        }else if(host == null) {
			    		output += "HTTP/1.1 400 Bad Request\r\n";
			    		isFail = true;
			    	}else if(fileURL.contains("..")) {
			    		output += "HTTP/1.1 403 Forbidden\r\n";
			    		isFail = true;
			    	}else if(!file.isFile()) {
			    		output += "HTTP/1.1 404 Not Found\r\n";
			    		isFail = true;
			    	}else if(!Files.isReadable(file.toPath())) {
			    		output += "HTTP/1.1 403 Forbidden\r\n";
			    		isFail = true;
			    	}else if(ims != null) {
			    		Instant imsInstant = null;
			    		Instant msInstant = null;
			    		try {
			    			DateTimeFormatter formatter = DateTimeFormatter.RFC_1123_DATE_TIME;
				    		OffsetDateTime odt = OffsetDateTime.parse(ims, formatter);
				    		imsInstant = odt.toInstant();
							msInstant = Files.getLastModifiedTime(file.toPath()).toInstant();
						} catch (IOException e) {
							e.printStackTrace();
						} catch(DateTimeParseException e) {
							e.printStackTrace();
						}
			    		
			    		
			    		Instant now = Instant.now();
			    		if(imsInstant == null || msInstant == null || imsInstant.isAfter(now) || msInstant.isAfter(imsInstant)) {
			    			output += "HTTP/1.1 200 OK\r\n";
			    		}else {
			    			output += "HTTP/1.1 304 Not Modified\r\n";
			    			isFail = true;
			    		}
			    		
			    	}else {
			    		output += "HTTP/1.1 200 OK\r\n";
			    	}
		    		
		    		if(strs[1].endsWith(".jpg") || strs[1].endsWith(".jpeg")) {
			    		output += "Content-Type: image/jpeg\r\n";
			    	}else if(strs[1].endsWith(".txt")) {
			    		output += "Content-Type: text/plain\r\n";
			    	}else if(strs[1].endsWith(".html")) {
			    		output += "Content-Type: text/html\r\n";
			    	}else {
			    		output += "Content-Type: application/octet-stream\r\n";
			    	}
		    		
		    		output += "Content-Length: " + (isFail ? "0" : file.length()) + "\r\n";
	    		}
	    		
	    	}
	    	
	    
	    	if(!isRouteExist) {
	    		output += "\r\n";
	    		System.out.println("path: " + firstLine.split(" ")[1]);
		    	System.out.print("output: " + output);
		    	PrintWriter out;
				try {
					out = new PrintWriter(sock.getOutputStream());
					out.print(output);
				    out.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}
			    
		    	if(!isFail && firstLine.split(" ")[0].equals("GET")) {
		    		// Send file content in this socket as the response body using DataOutputStream
			    	FileInputStream fileInputStream;
			    	DataOutputStream dataOutputStream;
					try {
						fileInputStream = new FileInputStream(file);
						dataOutputStream = new DataOutputStream(sock.getOutputStream());
						byte[] buffer = new byte[4 * 1024];
						int sendBytes = 0;
						while ((sendBytes = fileInputStream.read(buffer)) != -1) {
							// Send the file to Server Socket
							dataOutputStream.write(buffer, 0, sendBytes);
							dataOutputStream.flush();
						}
						
						// close the file here
						fileInputStream.close();
						//dataOutputStream.close();
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
		    	}    
	    	}
	    	
	    }
		
		
	    try {
			sock.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	private int checkDoubleCRLF(byte[] buf, int n) {
		for(int i = 0; i < n - 3; i++) {
			if(buf[i] == 13 && buf[i + 1] == 10 && buf[i + 2] == 13 && buf[i + 3] == 10) {
				return i + 2;
			}
		}
		return n;
	}
	
	private String handleFirstLine(String firstLine) throws IOException {
		
//        if(firstLine == null) {
//        	return "HTTP/1.1 400 Bad Request\r\n";
//        }
        //System.out.println(firstLine);
        String[] strs =  firstLine.split(" ");
        if(strs.length != 3) {
        	return "HTTP/1.1 400 Bad Request\r\n";
        }
        
        if(!strs[2].equals("HTTP/1.1")) {
        	return "HTTP/1.1 505 HTTP Version Not Supported\r\n";
        }else if(!strs[0].equals("GET") 
        		&& !strs[0].equals("HEAD") 
        		&& !strs[0].equals("POST")
        		&& !strs[0].equals("PUT")) {
        	return "HTTP/1.1 501 Not Implemented\r\n";
        }
//        else if(!strs[0].equals("GET") && !strs[0].equals("HEAD") ) {
//        	return "HTTP/1.1 405 Not Allowed\r\n";
//        }
        else {
        	return "Currently OK";
        }
	}
	
	
	private String pathMatch(Map<String, Route> routesMap, String path) {
		
		for(String ePath: routesMap.keySet()) {
			String eStrs[] = ePath.split("/");
			String strs[] = path.split("/");
			if(eStrs.length != strs.length) {
				continue;
			}
			boolean find = true;
			for(int i = 0; i < eStrs.length; i++) {
				String eS = eStrs[i];
				String s = strs[i];
				if(!eS.contains(":") && !eS.equals(s)) {
					find = false;
					break;
				}
			}
			
			if(find) {
				return ePath;
			}
		}
		
		return "";
	}
	
	private void findArgParms(String matchedPath, String path, Map<String, String> argParams) {
		
			String eStrs[] = matchedPath.split("/");
			String strs[] = path.split("/");
	
			for(int i = 0; i < eStrs.length; i++) {
				String eS = eStrs[i];
				String s = strs[i];
				if(eS.contains(":")) {
					argParams.put(eS.substring(1), s);
				}
			}	
	}
	
	private void findqueryParms(String queryPart, Map<String, String> queryParams) {
		if(queryPart.equals("")) {
			return;
		}
		String strs[] = queryPart.split("&");
		for(String s: strs) {
			String[] qs = s.split("=");
			if(qs.length == 2) {
				queryParams.put(java.net.URLDecoder.decode(qs[0]), java.net.URLDecoder.decode(qs[1]));
			}else {
				queryParams.put(java.net.URLDecoder.decode(qs[0]), "");
			}
		}
	}

}
