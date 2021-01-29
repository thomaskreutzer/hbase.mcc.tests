package org.apache.hadoop.hbase.test;

import com.cloudera.hbase.mcc.ConnectionFactoryMultiClusterWrapper;
import com.cloudera.hbase.mcc.MultiClusterConf;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


public class ConfigTest2 {
	static final Log log = LogFactory.getLog(ConfigTest2.class);
	
	public static void main(String[] args) throws Exception {
		MultiClusterConf multiClusterConf = new MultiClusterConf(true);
		Connection connection = ConnectionFactoryMultiClusterWrapper.createConnectionMultiUgi(multiClusterConf);

		String tableName = "test_table";
		String familyName = "cf1";
		int numberOfPuts = 5;
		int milliSecondsOfWait = 100;
		
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		for (int i = 1; i <= numberOfPuts; i++) {
			log.info("PUT");
			Put put = new Put(Bytes.toBytes(i % 10 + ".key." + StringUtils.leftPad(String.valueOf(i), 12)));
			put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("C"), Bytes.toBytes("Value:" + i));
			table.put(put);
			Thread.sleep(milliSecondsOfWait);
		}

		log.info("Closing Connection");
		connection.close();
		log.info(" - Connection Closed");
		
		
		System.exit(0);
	}
}
