package cis5550.flame;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.net.*;
import java.io.*;

import static cis5550.webserver.Server.*;

import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.IteratorToIterator;
import cis5550.flame.FlameRDD.StringToBoolean;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.flame.FlameRDD.StringToPairIterable;
import cis5550.kvs.*;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;
import cis5550.webserver.Request;

public class Worker extends cis5550.generic.Worker {
	static AtomicInteger nextId = new AtomicInteger(0);

	public static void main(String args[]) {
		if (args.length != 2) {
			System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
			System.exit(1);
		}

		int port = Integer.parseInt(args[0]);
		String server = args[1];
		startPingThread("" + port, server, "" + port);
		final File myJAR = new File("__worker" + port + "-current.jar");
		
		loadLexicon("lexicon.txt");
		
		
		port(port);

		post("/useJAR", (request, response) -> {
			FileOutputStream fos = new FileOutputStream(myJAR);
			fos.write(request.bodyAsBytes());
			fos.close();
			return "OK";
		});

		post("/rdd/flatMap", (request, response) -> {
			String inputTable = request.queryParams("input");
			String outputTable = request.queryParams("output");
			String kvsAddress = request.queryParams("kvsAddress");
			String startKey = request.queryParams("start");
			String endKey = request.queryParams("end");
			String isStartNull = request.queryParams("startNull");
			String isEndNull = request.queryParams("endNull");

			StringToIterable resultLamda = (StringToIterable) Serializer.byteArrayToObject(request.bodyAsBytes(),
					myJAR);
			//System.out.println("request.body: " + request.bodyAsBytes());
			//System.out.println("resultLamda: " + resultLamda);
			KVSClient kvs = new KVSClient(kvsAddress);

			String scanStart = (isStartNull != null ? null : startKey);
			String scanEnd = (isEndNull != null ? null : endKey);
			Iterator<Row> iter = kvs.scan(inputTable, scanStart, scanEnd);
			while (iter.hasNext()) {
				Row row = iter.next();

				Iterable<String> result = resultLamda.op(row.get("value"));
				if (result != null) {
					for (String value : result) {
						String rowKey = Hasher.hash(System.currentTimeMillis() + "-" + nextId.getAndIncrement());
						try {
							kvs.put(outputTable, rowKey, "value", value);
						} catch(Exception e) {
							e.printStackTrace();
						}
					}
				}

			}

			return "OK";
		});

		post("/rdd/mapToPair", (request, response) -> {
			String inputTable = request.queryParams("input");
			String outputTable = request.queryParams("output");
			String kvsAddress = request.queryParams("kvsAddress");
			String startKey = request.queryParams("start");
			String endKey = request.queryParams("end");
			String isStartNull = request.queryParams("startNull");
			String isEndNull = request.queryParams("endNull");

			StringToPair resultLamda = (StringToPair) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
//			System.out.println("request.body: " + request.bodyAsBytes());
//			System.out.println("resultLamda: " + resultLamda);
			KVSClient kvs = new KVSClient(kvsAddress);

			String scanStart = (isStartNull != null ? null : startKey);
			String scanEnd = (isEndNull != null ? null : endKey);
			Iterator<Row> iter = kvs.scan(inputTable, scanStart, scanEnd);
			while (iter.hasNext()) {
				Row row = iter.next();

				FlamePair result = resultLamda.op(row.get("value"));
				if (result != null) {
					String columnKey = row.key();
					try {
						kvs.put(outputTable, result._1(), columnKey, result._2());
					} catch(Exception e) {
						e.printStackTrace();
					}
				}

			}

			return "OK";
		});

		post("/pairrdd/foldByKey", (request, response) -> {
			String inputTable = request.queryParams("input");
			String outputTable = request.queryParams("output");
			String kvsAddress = request.queryParams("kvsAddress");
			String startKey = request.queryParams("start");
			String endKey = request.queryParams("end");
			String isStartNull = request.queryParams("startNull");
			String isEndNull = request.queryParams("endNull");
			String zeroElement = URLDecoder.decode(request.queryParams("zeroElement"), "UTF-8");

			TwoStringsToString resultLamda = (TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(),
					myJAR);
			//System.out.println("request.body: " + request.bodyAsBytes());
			//System.out.println("resultLamda: " + resultLamda);
			KVSClient kvs = new KVSClient(kvsAddress);

			String scanStart = (isStartNull != null ? null : startKey);
			String scanEnd = (isEndNull != null ? null : endKey);
			Iterator<Row> iter = kvs.scan(inputTable, scanStart, scanEnd);

			while (iter.hasNext()) {
				Row row = iter.next();

				String accumulator = zeroElement;
				for (String columnKey : row.columns()) {
					String value = row.get(columnKey);
					accumulator = resultLamda.op(accumulator, value);
				}
				try {
					kvs.put(outputTable, row.key(), "value", accumulator);
				} catch(Exception e) {
					e.printStackTrace();
				}

			}

			return "OK";
		});

		post("/fromTable", (request, response) -> {
			String inputTable = request.queryParams("input");
			String outputTable = request.queryParams("output");
			String kvsAddress = request.queryParams("kvsAddress");
			String startKey = request.queryParams("start");
			String endKey = request.queryParams("end");
			String isStartNull = request.queryParams("startNull");
			String isEndNull = request.queryParams("endNull");

			RowToString resultLamda = (RowToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
			//System.out.println("request.body: " + request.bodyAsBytes());
			//System.out.println("resultLamda: " + resultLamda);
			KVSClient kvs = new KVSClient(kvsAddress);

			String scanStart = (isStartNull != null ? null : startKey);
			String scanEnd = (isEndNull != null ? null : endKey);
			Iterator<Row> iter = kvs.scan(inputTable, scanStart, scanEnd);
			int testCount = 0;
			while (iter.hasNext()) {
				Row row = iter.next();
				testCount++;
				String result = resultLamda.op(row);
				if (result != null) {
					String rowKey = Hasher.hash(System.currentTimeMillis() + "-" + nextId.getAndIncrement());
					//System.out.println("rowKey: " + rowKey);
					try {
						kvs.put(outputTable, rowKey, "value", result);
					} catch(Exception e) {
						e.printStackTrace();
					}
				}
				//System.out.println("testCount: " + testCount);

			}
			//System.out.println("testCount: " + testCount);

			return "OK";
		});
		
		post("/rdd/flatMapToPair", (request, response) -> {
			String inputTable = request.queryParams("input");
			String outputTable = request.queryParams("output");
			String kvsAddress = request.queryParams("kvsAddress");
			String startKey = request.queryParams("start");
			String endKey = request.queryParams("end");
			String isStartNull = request.queryParams("startNull");
			String isEndNull = request.queryParams("endNull");

			StringToPairIterable resultLamda = (StringToPairIterable) Serializer.byteArrayToObject(request.bodyAsBytes(),
					myJAR);
			//System.out.println("request.body: " + request.bodyAsBytes());
			//System.out.println("resultLamda: " + resultLamda);
			KVSClient kvs = new KVSClient(kvsAddress);

			String scanStart = (isStartNull != null ? null : startKey);
			String scanEnd = (isEndNull != null ? null : endKey);
			Iterator<Row> iter = kvs.scan(inputTable, scanStart, scanEnd);
			while (iter.hasNext()) {
				Row row = iter.next();

				Iterable<FlamePair> result = resultLamda.op(row.get("value"));
				if (result != null) {
					for (FlamePair pair : result) {
						String columnKey = Hasher.hash(System.currentTimeMillis() + "-" + nextId.getAndIncrement());
						try {
							kvs.put(outputTable, pair._1(), columnKey, pair._2());
						} catch(Exception e) {
							e.printStackTrace();
						}
					}
				}

			}

			return "OK";
		});
		
		post("/pairrdd/flatMap", (request, response) -> {
			String inputTable = request.queryParams("input");
			String outputTable = request.queryParams("output");
			String kvsAddress = request.queryParams("kvsAddress");
			String startKey = request.queryParams("start");
			String endKey = request.queryParams("end");
			String isStartNull = request.queryParams("startNull");
			String isEndNull = request.queryParams("endNull");

			PairToStringIterable resultLamda = (PairToStringIterable) Serializer.byteArrayToObject(request.bodyAsBytes(),
					myJAR);
			//System.out.println("request.body: " + request.bodyAsBytes());
			//System.out.println("resultLamda: " + resultLamda);
			KVSClient kvs = new KVSClient(kvsAddress);

			String scanStart = (isStartNull != null ? null : startKey);
			String scanEnd = (isEndNull != null ? null : endKey);
			Iterator<Row> iter = kvs.scan(inputTable, scanStart, scanEnd);
			while (iter.hasNext()) {
				Row row = iter.next();
				
				for(String columnName: row.columns()) {
					Iterable<String> result = resultLamda.op(new FlamePair(row.key(), row.get(columnName)));
					if(result != null) {
						for(String value: result) {
							String rowKey = Hasher.hash(System.currentTimeMillis() + "-" + nextId.getAndIncrement());
							try {
								kvs.put(outputTable, rowKey, "value", value);
							} catch(Exception e) {
								e.printStackTrace();
							}
						}
					}
				}
				
			}

			return "OK";
		});
		
		post("/pairrdd/flatMapToPair", (request, response) -> {
			String inputTable = request.queryParams("input");
			String outputTable = request.queryParams("output");
			String kvsAddress = request.queryParams("kvsAddress");
			String startKey = request.queryParams("start");
			String endKey = request.queryParams("end");
			String isStartNull = request.queryParams("startNull");
			String isEndNull = request.queryParams("endNull");

			PairToPairIterable resultLamda = (PairToPairIterable) Serializer.byteArrayToObject(request.bodyAsBytes(),
					myJAR);
			//System.out.println("request.body: " + request.bodyAsBytes());
			//System.out.println("resultLamda: " + resultLamda);
			KVSClient kvs = new KVSClient(kvsAddress);

			String scanStart = (isStartNull != null ? null : startKey);
			String scanEnd = (isEndNull != null ? null : endKey);
			Iterator<Row> iter = kvs.scan(inputTable, scanStart, scanEnd);
			while (iter.hasNext()) {
				Row row = iter.next();
				
				for(String columnName: row.columns()) {
					Iterable<FlamePair> result = resultLamda.op(new FlamePair(row.key(), row.get(columnName)));
					if(result != null) {
						for(FlamePair value: result) {
							String columnKey = Hasher.hash(System.currentTimeMillis() + "-" + nextId.getAndIncrement());
							try {
								kvs.put(outputTable, value._1(), columnKey, value._2());
							} catch(Exception e) {
								e.printStackTrace();
							}
						}
					}
				}
				
			}

			return "OK";
		});
		
		
		post("/distinct", (request, response) -> {
			String inputTable = request.queryParams("input");
			String outputTable = request.queryParams("output");
			String kvsAddress = request.queryParams("kvsAddress");
			String startKey = request.queryParams("start");
			String endKey = request.queryParams("end");
			String isStartNull = request.queryParams("startNull");
			String isEndNull = request.queryParams("endNull");

			//System.out.println("request.body: " + request.bodyAsBytes());
			KVSClient kvs = new KVSClient(kvsAddress);

			String scanStart = (isStartNull != null ? null : startKey);
			String scanEnd = (isEndNull != null ? null : endKey);
			Iterator<Row> iter = kvs.scan(inputTable, scanStart, scanEnd);
			while (iter.hasNext()) {
				Row row = iter.next();
				try {
					kvs.put(outputTable, row.get("value"), "value", row.get("value"));
				} catch(Exception e) {
					e.printStackTrace();
				}
			}

			return "OK";
		});
		
		
		post("/join", (request, response) -> {
			//System.out.println("-------------------achive join worker");
			String inputTable1 = request.queryParams("input1");
			String inputTable2 = request.queryParams("input2");
			String outputTable = request.queryParams("output");
			String kvsAddress = request.queryParams("kvsAddress");
			String startKey = request.queryParams("start");
			String endKey = request.queryParams("end");
			String isStartNull = request.queryParams("startNull");
			String isEndNull = request.queryParams("endNull");

		
			//System.out.println("request.body: " + request.bodyAsBytes());
			KVSClient kvs = new KVSClient(kvsAddress);

			String scanStart = (isStartNull != null ? null : startKey);
			String scanEnd = (isEndNull != null ? null : endKey);
			Iterator<Row> iter = kvs.scan(inputTable1, scanStart, scanEnd);
//			System.out.println("inpoutTable1: " + inputTable1);
//			System.out.println("inpoutTable2: " + inputTable2);
			while (iter.hasNext()) {
				Row row1 = iter.next();
				Row row2 = kvs.getRow(inputTable2, row1.key());
//				System.out.println("inpoutTable1: " + inputTable1);
//				System.out.println("inpoutTable2: " + inputTable2);
//				System.out.println("row1 key: " + row1.key());
//				if(row2 != null)
//					System.out.println("row2 key: " + row2.key());
//				else {
//					System.out.println("row2 is null");
//				}
				if(row2 != null) {
					for(String columnName1: row1.columns()) {
						for(String columnName2: row2.columns()) {
							//System.out.println("-------------------achive outpuTable");
							String value = row1.get(columnName1) + "," + row2.get(columnName2);
							String newColName = Hasher.hash(columnName1 + ":" + columnName2);
							//System.out.println("-------------------achive outpuTable name: " + outputTable + "   value: " + value);
							try {
								kvs.put(outputTable, row1.key(), newColName, value);
							} catch(Exception e) {
								e.printStackTrace();
							}
						}
					}
				}
				
				
			}

			return "OK";
		});
		
		
		post("/fold", (request, response) -> {
			String inputTable = request.queryParams("input");
			String kvsAddress = request.queryParams("kvsAddress");
			String startKey = request.queryParams("start");
			String endKey = request.queryParams("end");
			String isStartNull = request.queryParams("startNull");
			String isEndNull = request.queryParams("endNull");
			String zeroElement = URLDecoder.decode(request.queryParams("zeroElement"), "UTF-8");

			TwoStringsToString resultLamda = (TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(),
					myJAR);
			//System.out.println("request.body: " + request.bodyAsBytes());
			//System.out.println("resultLamda: " + resultLamda);
			KVSClient kvs = new KVSClient(kvsAddress);

			String scanStart = (isStartNull != null ? null : startKey);
			String scanEnd = (isEndNull != null ? null : endKey);
			Iterator<Row> iter = kvs.scan(inputTable, scanStart, scanEnd);
			
			String accumulator = zeroElement;
			while (iter.hasNext()) {
				Row row = iter.next();
				accumulator = resultLamda.op(accumulator, row.get("value"));
			}
			
			response.header("value", accumulator);
			return "OK";
		});
		
		
		post("/rdd/filter", (request, response) -> {
			String inputTable = request.queryParams("input");
			String outputTable = request.queryParams("output");
			String kvsAddress = request.queryParams("kvsAddress");
			String startKey = request.queryParams("start");
			String endKey = request.queryParams("end");
			String isStartNull = request.queryParams("startNull");
			String isEndNull = request.queryParams("endNull");

			StringToBoolean resultLamda = (StringToBoolean) Serializer.byteArrayToObject(request.bodyAsBytes(),
					myJAR);
			//System.out.println("request.body: " + request.bodyAsBytes());
			//System.out.println("resultLamda: " + resultLamda);
			KVSClient kvs = new KVSClient(kvsAddress);

			String scanStart = (isStartNull != null ? null : startKey);
			String scanEnd = (isEndNull != null ? null : endKey);
			Iterator<Row> iter = kvs.scan(inputTable, scanStart, scanEnd);
			while (iter.hasNext()) {
				Row row = iter.next();
				Boolean result = resultLamda.op(row.get("value"));
				if (result != null && result == true) {
					try {
						kvs.put(outputTable, row.key(), "value", row.get("value"));
					} catch(Exception e) {
						e.printStackTrace();
					}
				}
			}

			return "OK";
		});
		
		
		post("/rdd/mapPartitions", (request, response) -> {
			String inputTable = request.queryParams("input");
			String outputTable = request.queryParams("output");
			String kvsAddress = request.queryParams("kvsAddress");
			String startKey = request.queryParams("start");
			String endKey = request.queryParams("end");
			String isStartNull = request.queryParams("startNull");
			String isEndNull = request.queryParams("endNull");

			IteratorToIterator resultLamda = (IteratorToIterator) Serializer.byteArrayToObject(request.bodyAsBytes(),
					myJAR);
			//System.out.println("request.body: " + request.bodyAsBytes());
			//System.out.println("resultLamda: " + resultLamda);
			KVSClient kvs = new KVSClient(kvsAddress);

			String scanStart = (isStartNull != null ? null : startKey);
			String scanEnd = (isEndNull != null ? null : endKey);
			Iterator<Row> iter = kvs.scan(inputTable, scanStart, scanEnd);
			List<String> orgValues = new ArrayList<>();
			while (iter.hasNext()) {
				Row row = iter.next();
				String value = row.get("value");
				orgValues.add(value);
			}
			
			Iterator<String> result = resultLamda.op(orgValues.iterator());
			while (result.hasNext()) {
				String value = result.next();
				String rowKey = Hasher.hash(System.currentTimeMillis() + "-" + nextId.getAndIncrement());
				try {
					kvs.put(outputTable, rowKey, "value", value);
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			
			return "OK";
		});
		
		post("/cogroup", (request, response) -> {
			String inputTable1 = request.queryParams("input1");
			String inputTable2 = request.queryParams("input2");
			String outputTable = request.queryParams("output");
			String kvsAddress = request.queryParams("kvsAddress");
			String startKey = request.queryParams("start");
			String endKey = request.queryParams("end");
			String isStartNull = request.queryParams("startNull");
			String isEndNull = request.queryParams("endNull");

			KVSClient kvs = new KVSClient(kvsAddress);

			String scanStart = (isStartNull != null ? null : startKey);
			String scanEnd = (isEndNull != null ? null : endKey);
			Iterator<Row> iter = kvs.scan(inputTable1, scanStart, scanEnd);
			while (iter.hasNext()) {
				Row row1 = iter.next();
				Row row2 = kvs.getRow(inputTable2, row1.key());
				List<String> list1 = new ArrayList<>();
				List<String> list2 = new ArrayList<>();
				
		
				for(String colName: row1.columns()) {
					list1.add(row1.get(colName));
				}
				
				if(row2 != null) {
					for(String colName: row2.columns()) {
						list2.add(row2.get(colName));
					}
				}
				
				String value = "[" + String.join(",", list1) + "],["+ String.join(",", list2) + "]";	
				try {
					kvs.put(outputTable, row1.key(), "value", value);
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			
			iter = kvs.scan(inputTable2, scanStart, scanEnd);
			while (iter.hasNext()) {
				Row row2 = iter.next();
				Row row1 = kvs.getRow(inputTable1, row2.key());
				List<String> list1 = new ArrayList<>();
				List<String> list2 = new ArrayList<>();
				
		
				for(String colName: row2.columns()) {
					list2.add(row2.get(colName));
				}
				
				if(row1 != null) {
					for(String colName: row1.columns()) {
						list1.add(row1.get(colName));
					}
				}
				
				String value = "[" + String.join(",", list1) + "],["+ String.join(",", list2) + "]";	
				try {
					kvs.put(outputTable, row2.key(), "value", value);
				} catch(Exception e) {
					e.printStackTrace();
				}
			}

			return "OK";
		});

	}
	
	private static void loadLexicon(String fileName) {
		BufferedReader reader;
		lexicon = new HashSet<>();
		try {
			reader = new BufferedReader(new FileReader(fileName));
			String line = reader.readLine();

			while (line != null) {
				lexicon.add(line);
				// read next line
				line = reader.readLine();
			}

			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("Lexicon loaded");
	}

	public static Set<String> lexicon;
}
