package cis5550.webserver;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class ResponseImpl implements Response{

	Map<String,String> headers;
	byte[] bodyRaw;
	String body;
	String contentType;
	Socket sock;
	int statusCode = 200;
	String reasonPhrase = "OK";
	boolean isWriteBefore = false;
	int recentBodyCall = 0;
	// 0: body or bodyRaw not called
	// 1: bodyRaw recently called
	// 2: body recently called
	
	int stage = 0;
	// 0: before stage
	// 1: route stage
	// 2: after stage
	
	ResponseImpl(Socket sock){
		this.sock = sock;
		this.headers = new HashMap<>();
	}
	@Override
	public void body(String body) {
		this.body = body;
		this.recentBodyCall = 2;
	}

	@Override
	public void bodyAsBytes(byte[] bodyArg) {
		this.bodyRaw = bodyArg;
		this.recentBodyCall = 1;
	}

	@Override
	public void header(String name, String value) {
		headers.put(name.toLowerCase(), value);
	}

	@Override
	public void type(String contentType) {
		this.contentType = contentType;
	}

	@Override
	public void status(int statusCode, String reasonPhrase) {
		this.statusCode = statusCode;
		this.reasonPhrase = reasonPhrase;
	}

	@Override
	public void write(byte[] b) {
		
		PrintWriter out;
		try {
			out = new PrintWriter(this.sock.getOutputStream());
			if(!isWriteBefore) {
				out.print("HTTP/1.1 " + statusCode + " " + reasonPhrase + "\r\n");
				for(String key: headers.keySet()) {
					out.print(key + ": " + headers.get(key) + "\r\n");
				}
				out.print("Connection: close\r\n");
				out.print("\r\n");
				out.flush();
				isWriteBefore = true;
			}
			//out.print(new String(b, StandardCharsets.UTF_8));
			//out.print(Base64.getEncoder().encodeToString(b));
			out.flush();
			this.sock.getOutputStream().write(b);
			//out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void redirect(String url, int responseCode) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void halt(int statusCode, String reasonPhrase) {
		if(stage == 0) {
			this.statusCode = statusCode;
			this.reasonPhrase = reasonPhrase;
		}
		
	}

}
