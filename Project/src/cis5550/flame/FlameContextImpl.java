package cis5550.flame;

import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.*;
import cis5550.tools.HTTP;
import cis5550.tools.Hasher;
import cis5550.tools.Partitioner;
import cis5550.tools.Serializer;
import cis5550.tools.HTTP.Response;
import cis5550.tools.Partitioner.Partition;

public class FlameContextImpl implements FlameContext {

	public String outputBody;
	static int keyRangesPerWorker = 1;
	static int nextJobID = 1;
	static int nextOutputTableID = 1;
	static Set<String> rddSets = new HashSet<>(Arrays.asList("/rdd/flatMap", "/fromTable", "/pairrdd/flatMap", "/distinct", "/rdd/filter", "/rdd/mapPartitions"));
	static Set<String> pairrddSets = new HashSet<>(Arrays.asList("/rdd/mapToPair", "/pairrdd/foldByKey", "/rdd/flatMapToPair", "/pairrdd/flatMapToPair", "/join", "/cogroup"));
	
	FlameContextImpl(String jarName) {
		outputBody = "";
	}

	@Override
	public KVSClient getKVS() {
		return Coordinator.kvs;
	}

	@Override
	public void output(String s) {
		outputBody += s;
	}

	@Override
	public FlameRDD parallelize(List<String> list) throws Exception {
		KVSClient kvs = getKVS();
		String tableName = "" + System.currentTimeMillis() + FlameContextImpl.nextJobID;
		FlameContextImpl.nextJobID++;
		for (int i = 0; i < list.size(); i++) {
			String rowKey = Hasher.hash(System.currentTimeMillis() + " " + i);
			kvs.put(tableName, rowKey, "value", list.get(i));
		}

		FlameRDDImpl fRDD = new FlameRDDImpl(tableName);
		return fRDD;
	}

	static public Object invokeOperation(String operationName, Serializable lambda, String oldTableName)
			throws Exception {
		//System.out.println("-------------------achive invokeOperation");
		String newTableName = "pt-" + FlameContextImpl.nextOutputTableID + "-" + System.currentTimeMillis();
		FlameContextImpl.nextOutputTableID++;

		Partitioner ptr = new Partitioner();
		ptr.setKeyRangesPerWorker(FlameContextImpl.keyRangesPerWorker);
		Vector<String> workers = new Vector<>();
		String resultKvsWorkers = new String(
				HTTP.doRequest("GET", "http://" + Coordinator.kvsAddress + "/workers", null).body());
		String[] lines = resultKvsWorkers.split("\n");
		for (int i = 1; i < lines.length; i++) {
			workers.add(lines[i]);
			//System.out.println("kvs worker: " + "i: " + lines[i]);
		}

		for (int i = 0; i < workers.size() - 1; i++) {
			String worker = workers.elementAt(i);
			//System.out.println("kvs worker: " + "i: "  + worker);
			String nextWorker = workers.elementAt(i + 1);
			String[] strs = worker.split(",");
			String id = strs[0];
			String address = strs[1];
			ptr.addKVSWorker(address, id, nextWorker.split(",")[0]);
		}

		String lastWorker = workers.elementAt(workers.size() - 1);
		//System.out.println("kvs worker: " + workers.elementAt(workers.size() - 1));
		String firstWorker = workers.elementAt(0);
		String[] strs = lastWorker.split(",");
		String id = strs[0];
		String address = strs[1];
		ptr.addKVSWorker(address, id, null);
		ptr.addKVSWorker(address, null, firstWorker.split(",")[0]);
		
		List<String> cooWorkers = Coordinator.getWorkers();
		for (String worker : cooWorkers) {
			int flameAddressInd = worker.indexOf(":");
			String flameAddress = worker.substring(flameAddressInd + 1);
			//System.out.println("flameAddressInd: " + flameAddress);
			ptr.addFlameWorker(flameAddress);
		}
		
		Vector<Partition> partitions = ptr.assignPartitions();
		
		Thread threads[] = new Thread[partitions.size()];
		String results[] = new String[partitions.size()];
		for (int i = 0; i < partitions.size(); i++) {
			final int j = i;
			threads[i] = new Thread("flame worker #" + (i + 1)) {
				public void run() {
					try {
						Partition p = partitions.elementAt(j);
						//System.out.println("assignedFlameWorker: " + p.assignedFlameWorker);
						String url = "http://" + p.assignedFlameWorker + operationName + "?";
						url += "input=" + URLEncoder.encode(oldTableName, StandardCharsets.UTF_8.toString()) + "&";
						url += "output=" + URLEncoder.encode(newTableName, StandardCharsets.UTF_8.toString()) + "&";
						if (p.fromKey == null) {
							url += "startNull=" + URLEncoder.encode("null", StandardCharsets.UTF_8.toString()) + "&";
						}else {
							url += "start=" + URLEncoder.encode(p.fromKey, StandardCharsets.UTF_8.toString()) + "&";
						}
						if (p.toKeyExclusive == null) {
							url += "endNull=" + URLEncoder.encode("null", StandardCharsets.UTF_8.toString()) + "&";
						}else {
							url += "end=" + URLEncoder.encode(p.toKeyExclusive, StandardCharsets.UTF_8.toString()) + "&";
						}
						url += "kvsAddress=" + URLEncoder.encode(Coordinator.kvsAddress, StandardCharsets.UTF_8.toString());
						
						
						//System.out.println("url: " + url);
						results[j] = new String(
								HTTP.doRequest("POST", url, Serializer.objectToByteArray(lambda)).body());
						//System.out.println("serialized body in coor: " + Serializer.objectToByteArray(lambda));
					} catch (Exception e) {
						results[j] = "Exception: " + e;
						e.printStackTrace();
					}
				}
			};
			threads[i].start();
		}

		// Wait for all the uploads to finish
		for (int i = 0; i < threads.length; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException ie) {
			}
		}

		for (int i = 0; i < results.length; i++) {
			if (!results[i].equals("OK")) {
				throw new Exception("flame worker #" + (i + 1) + "failed." + "We got: " + results[i]);
			}
		}

		if (rddSets.contains(operationName)) {
			return new FlameRDDImpl(newTableName);
		} else if (pairrddSets.contains(operationName)) {
			return new FlamePairRDDImpl(newTableName);
		} else {
			throw new Exception("Illegal operation name");
		}

	}
	
	
	static public Object invokeOperation(String operationName, Serializable lambda, String oldTableName, String zeroElement)
			throws Exception {
		String newTableName = "pt-" + FlameContextImpl.nextOutputTableID + "-" + System.currentTimeMillis();
		FlameContextImpl.nextOutputTableID++;

		Partitioner ptr = new Partitioner();
		ptr.setKeyRangesPerWorker(FlameContextImpl.keyRangesPerWorker);
		
		Vector<String> workers = new Vector<>();
		String resultKvsWorkers = new String(
				HTTP.doRequest("GET", "http://" + Coordinator.kvsAddress + "/workers", null).body());
		String[] lines = resultKvsWorkers.split("\n");
		for (int i = 1; i < lines.length; i++) {
			workers.add(lines[i]);
			//System.out.println("kvs worker: " + "i: " + lines[i]);
		}

		for (int i = 0; i < workers.size() - 1; i++) {
			String worker = workers.elementAt(i);
			//System.out.println("kvs worker: " + worker);
			String nextWorker = workers.elementAt(i + 1);
			String[] strs = worker.split(",");
			String id = strs[0];
			String address = strs[1];
			ptr.addKVSWorker(address, id, nextWorker.split(",")[0]);
		}

		String lastWorker = workers.elementAt(workers.size() - 1);
		//System.out.println("kvs worker: " + workers.elementAt(workers.size() - 1));
		String firstWorker = workers.elementAt(0);
		String[] strs = lastWorker.split(",");
		String id = strs[0];
		String address = strs[1];
		ptr.addKVSWorker(address, id, null);
		ptr.addKVSWorker(address, null, firstWorker.split(",")[0]);
		
		List<String> cooWorkers = Coordinator.getWorkers();
		for (String worker : cooWorkers) {
			//System.out.println("flame worker: " + worker);
			//ptr.addFlameWorker(worker);
			
			int flameAddressInd = worker.indexOf(":");
			String flameAddress = worker.substring(flameAddressInd + 1);
			ptr.addFlameWorker(flameAddress);
		}
		Vector<Partition> partitions = ptr.assignPartitions();

//		if(partitions == null) {
//			String eRes = "";
//			for(String s: ptr.kvsWorkers) {
//				eRes += s + "\n";
//			}
//			for(String s: ptr.flameWorkers) {
//				eRes += s  + "\n";
//			}
//			System.out.println("Exception: " + eRes);
//			for (String worker : cooWorkers) {
//				int flameAddressInd = worker.indexOf(":");
//				String flameAddress = worker.substring(flameAddressInd + 1);
//				System.out.println("flameAddressInd: " + flameAddress);
//				//ptr.addFlameWorker(flameAddress);
//			}
//			if (rddSets.contains(operationName)) {
//				return new FlameRDDImpl(newTableName);
//			} else if (pairrddSets.contains(operationName)) {
//				return new FlamePairRDDImpl(newTableName);
//			} 
//		}
		Thread threads[] = new Thread[partitions.size()];
		String results[] = new String[partitions.size()];
		for (int i = 0; i < partitions.size(); i++) {
			final int j = i;
			threads[i] = new Thread("flame worker #" + (i + 1)) {
				public void run() {
					try {
						Partition p = partitions.elementAt(j);
						String url = "http://" + p.assignedFlameWorker + operationName + "?";
						url += "input=" + URLEncoder.encode(oldTableName, StandardCharsets.UTF_8.toString()) + "&";
						url += "output=" + URLEncoder.encode(newTableName, StandardCharsets.UTF_8.toString()) + "&";
						url += "kvsAddress=" + URLEncoder.encode(Coordinator.kvsAddress, StandardCharsets.UTF_8.toString()) + "&";
						//url += "zeroElement=" + zeroElement + "&";
						if (p.fromKey == null) {
							url += "startNull=" + URLEncoder.encode("null", StandardCharsets.UTF_8.toString()) + "&";
						}else {
							url += "start=" + URLEncoder.encode(p.fromKey, StandardCharsets.UTF_8.toString()) + "&";
						}
						if (p.toKeyExclusive == null) {
							url += "endNull=" + URLEncoder.encode("null", StandardCharsets.UTF_8.toString()) + "&";
						}else {
							url += "end=" + URLEncoder.encode(p.toKeyExclusive, StandardCharsets.UTF_8.toString()) + "&";
						}
						url += "zeroElement=" + URLEncoder.encode(zeroElement, StandardCharsets.UTF_8.toString());
						//System.out.println("url: " + url);
						results[j] = new String(
								HTTP.doRequest("POST", url, Serializer.objectToByteArray(lambda)).body());
						//System.out.println("serialized body in coor: " + Serializer.objectToByteArray(lambda));
					} catch (Exception e) {
						results[j] = "Exception: " + e;
						e.printStackTrace();
					}
				}
			};
			threads[i].start();
		}

		// Wait for all the uploads to finish
		for (int i = 0; i < threads.length; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException ie) {
			}
		}

		for (int i = 0; i < results.length; i++) {
			if (!results[i].equals("OK")) {
				throw new Exception("flame worker #" + (i + 1) + "failed." + "We got: " + results[i]);
			}
		}

		if (rddSets.contains(operationName)) {
			return new FlameRDDImpl(newTableName);
		} else if (pairrddSets.contains(operationName)) {
			return new FlamePairRDDImpl(newTableName);
		} else {
			throw new Exception("Illegal operation name");
		}

	}
	
	static public Object invokeOperationJoin(String operationName, String tableName1, String tableName2)
			throws Exception {
		String newTableName = "pt-" + FlameContextImpl.nextOutputTableID + "-" + System.currentTimeMillis();
		FlameContextImpl.nextOutputTableID++;

		Partitioner ptr = new Partitioner();
		ptr.setKeyRangesPerWorker(FlameContextImpl.keyRangesPerWorker);
		
		Vector<String> workers = new Vector<>();
		String resultKvsWorkers = new String(
				HTTP.doRequest("GET", "http://" + Coordinator.kvsAddress + "/workers", null).body());
		String[] lines = resultKvsWorkers.split("\n");
		for (int i = 1; i < lines.length; i++) {
			workers.add(lines[i]);
			//System.out.println("kvs worker: " + "i: " + lines[i]);
		}

		for (int i = 0; i < workers.size() - 1; i++) {
			String worker = workers.elementAt(i);
			//System.out.println("kvs worker: " + worker);
			String nextWorker = workers.elementAt(i + 1);
			String[] strs = worker.split(",");
			String id = strs[0];
			String address = strs[1];
			ptr.addKVSWorker(address, id, nextWorker.split(",")[0]);
		}

		String lastWorker = workers.elementAt(workers.size() - 1);
		//System.out.println("kvs worker: " + workers.elementAt(workers.size() - 1));
		String firstWorker = workers.elementAt(0);
		String[] strs = lastWorker.split(",");
		String id = strs[0];
		String address = strs[1];
		ptr.addKVSWorker(address, id, null);
		ptr.addKVSWorker(address, null, firstWorker.split(",")[0]);
		
		List<String> cooWorkers = Coordinator.getWorkers();
		for (String worker : cooWorkers) {
			//System.out.println("flame worker: " + worker);
			//ptr.addFlameWorker(worker);
			int flameAddressInd = worker.indexOf(":");
			String flameAddress = worker.substring(flameAddressInd + 1);
			ptr.addFlameWorker(flameAddress);
		}

		Vector<Partition> partitions = ptr.assignPartitions();
		
		//System.out.println("partition size: " + partitions.size());
		Thread threads[] = new Thread[partitions.size()];
		String results[] = new String[partitions.size()];
		for (int i = 0; i < partitions.size(); i++) {
			final int j = i;
			threads[i] = new Thread("flame worker #" + (i + 1)) {
				public void run() {
					try {
						Partition p = partitions.elementAt(j);
						String url = "http://" + p.assignedFlameWorker + operationName + "?";
						url += "input1=" + URLEncoder.encode(tableName1, StandardCharsets.UTF_8.toString()) + "&";
						url += "input2=" + URLEncoder.encode(tableName2, StandardCharsets.UTF_8.toString()) + "&";
						url += "output=" + URLEncoder.encode(newTableName, StandardCharsets.UTF_8.toString()) + "&";
						if (p.fromKey == null) {
							url += "startNull=" + URLEncoder.encode("null", StandardCharsets.UTF_8.toString()) + "&";
						}else {
							url += "start=" + URLEncoder.encode(p.fromKey, StandardCharsets.UTF_8.toString()) + "&";
						}
						if (p.toKeyExclusive == null) {
							url += "endNull=" + URLEncoder.encode("null", StandardCharsets.UTF_8.toString())+ "&";
						}else {
							url += "end=" + URLEncoder.encode(p.toKeyExclusive, StandardCharsets.UTF_8.toString()) + "&";
						}
						url += "kvsAddress=" + URLEncoder.encode(Coordinator.kvsAddress, StandardCharsets.UTF_8.toString());
			
						//System.out.println("partition url: " + url);
						results[j] = new String(
								HTTP.doRequest("POST", url, null).body());
						
					} catch (Exception e) {
						results[j] = "Exception: " + e;
						e.printStackTrace();
					}
				}
			};
			threads[i].start();
		}

		// Wait for all the uploads to finish
		for (int i = 0; i < threads.length; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException ie) {
			}
		}

		for (int i = 0; i < results.length; i++) {
			if (!results[i].equals("OK")) {
				throw new Exception("flame worker #" + (i + 1) + "failed." + "We got: " + results[i]);
			}
		}

		if (rddSets.contains(operationName)) {
			return new FlameRDDImpl(newTableName);
		} else if (pairrddSets.contains(operationName)) {
			return new FlamePairRDDImpl(newTableName);
		} else {
			throw new Exception("Illegal operation name");
		}

	}
	
	static public String invokeOperationFold(String operationName, Serializable lambda, String tableName, String zeroElement)
			throws Exception {
		
		Partitioner ptr = new Partitioner();
		ptr.setKeyRangesPerWorker(FlameContextImpl.keyRangesPerWorker);
		
		Vector<String> workers = new Vector<>();
		String resultKvsWorkers = new String(
				HTTP.doRequest("GET", "http://" + Coordinator.kvsAddress + "/workers", null).body());
		String[] lines = resultKvsWorkers.split("\n");
		for (int i = 1; i < lines.length; i++) {
			workers.add(lines[i]);
			//System.out.println("kvs worker: " + "i: " + lines[i]);
		}

		for (int i = 0; i < workers.size() - 1; i++) {
			String worker = workers.elementAt(i);
			//System.out.println("kvs worker: " + worker);
			String nextWorker = workers.elementAt(i + 1);
			String[] strs = worker.split(",");
			String id = strs[0];
			String address = strs[1];
			ptr.addKVSWorker(address, id, nextWorker.split(",")[0]);
		}

		String lastWorker = workers.elementAt(workers.size() - 1);
		//System.out.println("kvs worker: " + workers.elementAt(workers.size() - 1));
		String firstWorker = workers.elementAt(0);
		String[] strs = lastWorker.split(",");
		String id = strs[0];
		String address = strs[1];
		ptr.addKVSWorker(address, id, null);
		ptr.addKVSWorker(address, null, firstWorker.split(",")[0]);
		
		List<String> cooWorkers = Coordinator.getWorkers();
		for (String worker : cooWorkers) {
			//System.out.println("flame worker: " + worker);
			//ptr.addFlameWorker(worker);
			int flameAddressInd = worker.indexOf(":");
			String flameAddress = worker.substring(flameAddressInd + 1);
			ptr.addFlameWorker(flameAddress);
		}

		Vector<Partition> partitions = ptr.assignPartitions();
//		if(partitions == null) {
//			String eRes = "";
//			for(String s: ptr.kvsWorkers) {
//				eRes += s + "\n";
//			}
//			for(String s: ptr.flameWorkers) {
//				eRes += s  + "\n";
//			}
//			System.out.println("Exception: " + eRes);
//			for (String worker : cooWorkers) {
//				int flameAddressInd = worker.indexOf(":");
//				String flameAddress = worker.substring(flameAddressInd + 1);
//				System.out.println("flameAddressInd: " + flameAddress);
//				//ptr.addFlameWorker(flameAddress);
//			}
//			return zeroElement;
//		}
		Thread threads[] = new Thread[partitions.size()];
		String results[] = new String[partitions.size()];
		String values[] = new String[partitions.size()];
		
		for (int i = 0; i < partitions.size(); i++) {
			final int j = i;
			threads[i] = new Thread("flame worker #" + (i + 1)) {
				public void run() {
					try {
						Partition p = partitions.elementAt(j);
						String url = "http://" + p.assignedFlameWorker + operationName + "?";
						url += "input=" + URLEncoder.encode(tableName, StandardCharsets.UTF_8.toString()) + "&";
						url += "kvsAddress=" + URLEncoder.encode(Coordinator.kvsAddress, StandardCharsets.UTF_8.toString()) + "&";
						url += "zeroElement=" + URLEncoder.encode(zeroElement, StandardCharsets.UTF_8.toString()) + "&";
						if (p.fromKey == null) {
							url += "startNull=" + URLEncoder.encode("null" , StandardCharsets.UTF_8.toString())+ "&";
						}else {
							url += "start=" + URLEncoder.encode(p.fromKey, StandardCharsets.UTF_8.toString()) + "&";
						}
						if (p.toKeyExclusive == null) {
							url += "endNull=" + URLEncoder.encode("null" , StandardCharsets.UTF_8.toString()) + "&";
						}else {
							url += "end=" + URLEncoder.encode(p.toKeyExclusive, StandardCharsets.UTF_8.toString()) + "&";
						}
						url += "zeroElement=" + URLEncoder.encode(zeroElement, StandardCharsets.UTF_8.toString());
						//System.out.println("url: " + url);
						Response response = HTTP.doRequest("POST", url, Serializer.objectToByteArray(lambda));
						results[j] = new String(response.body());
						values[j] = response.headers().get("value");
					} catch (Exception e) {
						results[j] = "Exception: " + e;
						e.printStackTrace();
					}
				}
			};
			threads[i].start();
		}

		// Wait for all the uploads to finish
		for (int i = 0; i < threads.length; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException ie) {
			}
		}

		for (int i = 0; i < results.length; i++) {
			if (!results[i].equals("OK")) {
				throw new Exception("flame worker #" + (i + 1) + "failed." + "We got: " + results[i]);
			}
		}
		
		String accumulator = zeroElement;
		for (String v: values) {
			accumulator = ((TwoStringsToString)lambda).op(accumulator, v);
		}
		return accumulator;
		

	}

	@Override
	public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
		return (FlameRDD)FlameContextImpl.invokeOperation("/fromTable", lambda, tableName);
	}

	@Override
	public void setConcurrencyLevel(int keyRangesPerWorker) {
		FlameContextImpl.keyRangesPerWorker = keyRangesPerWorker;
	}

}
