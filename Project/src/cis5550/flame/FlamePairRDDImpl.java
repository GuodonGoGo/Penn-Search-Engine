package cis5550.flame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

public class FlamePairRDDImpl implements FlamePairRDD{
	public String tableName;
	public FlamePairRDDImpl(String tableName){
		this.tableName = tableName;
	}
	@Override
	public List<FlamePair> collect() throws Exception {
		List<FlamePair> list = new ArrayList<>();
		KVSClient kvs = Coordinator.kvs;
		Iterator<Row> iter = kvs.scan(tableName);
		while(iter.hasNext()) {
			Row row = iter.next();
			for(String columnKey: row.columns()) {
				String value = row.get(columnKey);
				list.add(new FlamePair(row.key(), value));
			}
		}
		return list;
	}

	@Override
	public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
		return (FlamePairRDD)FlameContextImpl.invokeOperation("/pairrdd/foldByKey", lambda, tableName, zeroElement);
	}
	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		KVSClient kvs = Coordinator.kvs;
		kvs.rename(tableName, tableNameArg);
		tableName = tableNameArg;
	}
	@Override
	public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
		return (FlameRDD)FlameContextImpl.invokeOperation("/pairrdd/flatMap", lambda, tableName);
	}
	@Override
	public void destroy() throws Exception {
		KVSClient kvs = Coordinator.kvs;
		kvs.delete(tableName);
	}
	@Override
	public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
		return (FlamePairRDD)FlameContextImpl.invokeOperation("/pairrdd/flatMapToPair", lambda, tableName);
	}
	@Override
	public FlamePairRDD join(FlamePairRDD other) throws Exception {
		return (FlamePairRDD)FlameContextImpl.invokeOperationJoin("/join", tableName, ((FlamePairRDDImpl)other).tableName);
	}
	@Override
	public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
		return (FlamePairRDD)FlameContextImpl.invokeOperationJoin("/cogroup", tableName, ((FlamePairRDDImpl)other).tableName);
	}
}
