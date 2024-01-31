package cis5550.webserver;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ServerSocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import cis5550.tools.Logger;

public class Server implements Runnable{
	
	private static final Logger logger = Logger.getLogger(Server.class);
	private static final int NUM_WORKERS = 100;
	
	public int port = 80;
	public int securePortNo;
	public String fileName = "";
	
	public static Map<String, Route> getTable = new HashMap<>();
	public static Map<String, Route> putTable = new HashMap<>();
	public static Map<String, Route> postTable = new HashMap<>();
	public static Route routeBefore = null;
	public static Route routeAfter = null;
	public static String currentHost = null;
	public static Map<String, Map<String, Route>> hostGetTable = new HashMap<>();
	public static Map<String, Map<String, Route>> hostPutTable = new HashMap<>();
	public static Map<String, Map<String, Route>> hostPostTable = new HashMap<>();
	
	
	
	
	public void run() {
		
//		if(args.length != 2) {
//			System.out.println("Written by Junwei Yang");
//		}
		
//		int port = Integer.valueOf(args[0]);
//		String fileName = args[1];
		
		final ServerSocket tlsSsock;
		Runnable tslRunnable = null;
		String pwd = "secret";
		KeyStore keyStore;
		try {
			keyStore = KeyStore.getInstance("JKS");
			keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
			KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
			keyManagerFactory.init(keyStore, pwd.toCharArray());
			SSLContext sslContext = SSLContext.getInstance("TLS");
			sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
			ServerSocketFactory factory = sslContext.getServerSocketFactory();
			tlsSsock = factory.createServerSocket(securePortNo);
			tslRunnable =
				    new Runnable(){
				        public void run(){
				        	serverLoop(tlsSsock);
				        }
				    }
			;
		} catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException | UnrecoverableKeyException | KeyManagementException e) {
			e.printStackTrace();
		}
		
		Runnable regularRunnable = null;
		final ServerSocket ssock;
		try {
			ssock = new ServerSocket(port);
			regularRunnable =
				    new Runnable(){
				        public void run(){
				        	serverLoop(ssock);
				        }
				    }
			;
		} catch (IOException e) {
			e.printStackTrace();
		}
		
//		Runnable cleanRunnable =
//			    new Runnable(){
//			        public void run(){
//			        	cleanPeriodicaly();
//			        }
//			    }
//		;
		
		Thread thread1 = new Thread(regularRunnable);
		Thread thread2 = new Thread(tslRunnable);
		//Thread thread3 = new Thread(cleanRunnable);
		
		thread1.start();
		thread2.start();
		//thread3.start();
		
		
		
		
		//pool.shutdown();
	    //ssock.close();


	}
	
	static Server server = null;
	static boolean flag = false;
	
	public static void get(String s, Route r) {
		if(currentHost == null) {
			getTable.put(s, r);
		}else {
			Map<String, Route> map = hostGetTable.getOrDefault(currentHost, new HashMap<>());
			map.put(s, r);
			hostGetTable.put(currentHost, map);
		}

		if(server == null) {
			server = new Server();
		}
		
		if(!flag) {
			flag = true;
			
			Thread thread = new Thread(server);
			thread.start();
			
		}
		
	}
	
	public static void post(String s, Route r) {
		if(currentHost == null) {
			postTable.put(s, r);
		}else {
			Map<String, Route> map = hostPostTable.getOrDefault(currentHost, new HashMap<>());
			map.put(s, r);
			hostPostTable.put(currentHost, map);
		}
		
		if(server == null) {
			server = new Server();
		}
		if(!flag) {
			flag = true;
			Thread thread = new Thread(server);
			thread.start();
		}
		
	}
	
	public static void put(String s, Route r) {
		if(currentHost == null) {
			putTable.put(s, r);
		}else {
			Map<String, Route> map = hostPutTable.getOrDefault(currentHost, new HashMap<>());
			map.put(s, r);
			hostPutTable.put(currentHost, map);
		}
		
		if(server == null) {
			server = new Server();
		}
		if(!flag) {
			flag = true;
			Thread thread = new Thread(server);
			thread.start();
		}
	}
	
	public static void before(Route r) {
		routeBefore = r;
	}
	
	public static void after(Route r) {
		routeAfter = r;
	}
	
	public static void host(String s) {
		currentHost = s.toLowerCase();
	}
	
	public static void port(int N) {
		if(server == null) {
			server = new Server();
		}
		server.port = N;
	}
	
	public static void securePort(int N) {
		if(server == null) {
			server = new Server();
		}
		server.securePortNo = N;
		
	}
	
	public static class staticFiles{
		public static void location(String s) {
			if(server == null) {
				server = new Server();
			}
			server.fileName = s;
		}
		 
		 
	}
	
	private void serverLoop(ServerSocket ssock) {
		ExecutorService pool = Executors.newFixedThreadPool(NUM_WORKERS);
		while(true) {
			try {
				Socket sock = ssock.accept();
				SocketRun socketRun = new SocketRun(sock, fileName, server);
				pool.execute(socketRun);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	

}
