package cis5550.generic;

import static cis5550.webserver.Server.get;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class Work {
	String id;
	String ip;
	String port;
	long lastTime;

	Work(String id, String ip, String port, long lastTime) {
		this.id = id;
		this.ip = ip;
		this.port = port;
		this.lastTime = lastTime;
	}
}

public class Coordinator {
	public static Map<String, Work> workers = new HashMap<>();

	public static List<String> getWorkers() throws Exception {
		List<String> ans = new ArrayList<>();
		long curTime = System.currentTimeMillis();
		for (String key : workers.keySet()) {
			Work worker = workers.get(key);
			// if(worker.lastTime + 300000 > curTime) {
			ans.add(key + ":" + worker.ip + ":" + worker.port);
			// }
		}

		Collections.sort(ans, (a, b) -> {
			int index1 = a.indexOf(":");
			int index2 = b.indexOf(":");
			return a.substring(0, index1).compareTo(b.substring(0, index2));
		});

		if (ans.size() == 0) {
			String eRes = "curTime: " + curTime;
			for (String key : workers.keySet()) {
				Work worker = workers.get(key);
				eRes += "id:" + worker.id + " ip:" + worker.ip + " port:" + worker.port + " time:" + worker.lastTime
						+ "\n";

			}
			throw new Exception("Excecption when getWorker is Empty: " + eRes);
		}
		return ans;
	}

	public static String workerTable() throws Exception {
		String ans = "<table>";
		ans += "<tr>" + "<th>ID</th>" + "<th>IP</th>" + "<th>Port</th>" + "</tr>";
		for (String worker : getWorkers()) {
			String[] strs = worker.split(":");
			String id = strs[0];
			String ip = strs[1];
			String port = strs[2];
			ans += "<tr>"
					+ "<th><a href=\"http://" + ip + ":" + port + "/\">" + id + "</a>" + "</th>"
					+ "<th>" + ip + "</th>"
					+ "<th>" + port + "</th>"
					+ "</tr>";
		}
		return ans;
	}

	public static void registerRoutes() {
		get("/ping", (req, res) -> {
			String id = req.queryParams("id");
			String port = req.queryParams("port");
			if (id == null || port == null) {
				res.status(400, "Bad Request");
				return null;
			} else {
				workers.put(id, new Work(id, req.ip(), port, System.currentTimeMillis()));
				return "OK";
			}
		});

		get("/workers", (req, res) -> {
			String ans = "";
			ans += getWorkers().size() + "\n";
			for (String worker : getWorkers()) {
				String[] strs = worker.split(":");
				ans += strs[0] + "," + strs[1] + ":" + strs[2] + "\n";
			}
			return ans;
		});
	}
}
