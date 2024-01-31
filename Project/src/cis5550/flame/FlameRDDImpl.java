package cis5550.flame;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

public class FlameRDDImpl implements FlameRDD{
	
	public String tableName;
	
	public FlameRDDImpl(String tableName){
		this.tableName = tableName;
	}
	
	@Override
	public List<String> collect() throws Exception {
		List<String> list = new ArrayList<>();
		KVSClient kvs = Coordinator.kvs;
		Iterator<Row> iter = kvs.scan(tableName);
		while(iter.hasNext()) {
			Row row = iter.next();
			String s = row.get("value");
			list.add(s);
		}
		return list;
	}

	@Override
	public FlameRDD flatMap(StringToIterable lambda) throws Exception {
		return (FlameRDDImpl)FlameContextImpl.invokeOperation("/rdd/flatMap", lambda, tableName);
	}

	@Override
	public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
		//System.out.println("-------------------achive mapToPair");
		return (FlamePairRDD)FlameContextImpl.invokeOperation("/rdd/mapToPair", lambda, tableName);
	}

	@Override
	public FlameRDD intersection(FlameRDD r) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FlameRDD sample(double f) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FlamePairRDD groupBy(StringToString lambda) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int count() throws Exception {
		KVSClient kvs = Coordinator.kvs;
		return kvs.count(tableName);
	}

	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		KVSClient kvs = Coordinator.kvs;
		kvs.rename(tableName, tableNameArg);
		tableName = tableNameArg;
	}

	@Override
	public FlameRDD distinct() throws Exception {
		return (FlameRDD)FlameContextImpl.invokeOperation("/distinct", null, tableName);
	}

	@Override
	public void destroy() throws Exception {
		KVSClient kvs = Coordinator.kvs;
		kvs.delete(tableName);
	}

	@Override
	public Vector<String> take(int num) throws Exception {
		Vector<String> ans = new Vector<>();
		KVSClient kvs = Coordinator.kvs;
		Iterator<Row> iter = kvs.scan(tableName);
		int count = 0;
		while(iter.hasNext() && count < num) {
			Row row = iter.next();
			String s = row.get("value");
			ans.add(s);
			count++;
		}
		return ans;
	}

	@Override
	public String fold(String zeroElement, TwoStringsToString lambda) throws Exception {
		return FlameContextImpl.invokeOperationFold("/fold", lambda, tableName, zeroElement);
	}

	@Override
	public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
		return (FlamePairRDD)FlameContextImpl.invokeOperation("/rdd/flatMapToPair", lambda, tableName);
	}

	@Override
	public FlameRDD filter(StringToBoolean lambda) throws Exception {
		return (FlameRDD)FlameContextImpl.invokeOperation("/rdd/filter", lambda, tableName);
	}

	@Override
	public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
		return (FlameRDD)FlameContextImpl.invokeOperation("/rdd/mapPartitions", lambda, tableName);
	}

}
