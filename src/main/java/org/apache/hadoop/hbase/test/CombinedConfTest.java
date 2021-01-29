package org.apache.hadoop.hbase.test;

import java.io.File;
import java.io.FileInputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.hadoop.security.UserGroupInformation;
import com.cloudera.hbase.mcc.*;

public class CombinedConfTest {
	static Logger log = Logger.getLogger(CombinedConfTest.class);
	public static void main(String[] args) throws Exception {
		//String HBASE_CLIENT_CONNECTION_IMPL = "hbase.client.connection.impl"; //This constant seems to be moving around in versions of HBASE for now just use hard coded
		//String CONNECTION_IMPL = "com.cloudera.hbase.mcc.ConnectionMultiCluster";
		
		//Going to try and create a connection using the same keytab for two clusters with single keytab using combine/Split
		Configuration primary = HBaseConfiguration.create();
		primary.addResource( new FileInputStream(new File("/Users/tkreutzer/Documents/clients/citi/hbase/cluster1/hbase-site.xml")) );
		primary.addResource( new FileInputStream(new File("/Users/tkreutzer/Documents/clients/citi/hbase/cluster1/core-site.xml")) );
		primary.set("hadoop.security.authentication", "kerberos");
		primary.set("hbase.client.retries.number", "1"); //Override Default Parameters
		primary.set("hbase.client.pause", "1"); //Override Default Parameters
		//primary.set(HBASE_CLIENT_CONNECTION_IMPL,CONNECTION_IMPL);
		
		UserGroupInformation.setConfiguration(primary);
		UserGroupInformation.loginUserFromKeytab("cluster1tls/ccycloud-1.tkhbasetls1.root.hwx.site@TLS1.COM","/Users/tkreutzer/Documents/clients/citi/hbase/cluster1/cluster1tls_host.keytab");
		
		Configuration failover = HBaseConfiguration.create();
		failover.addResource( new FileInputStream(new File("/Users/tkreutzer/Documents/clients/citi/hbase/cluster2/hbase-site.xml")) );
		failover.addResource( new FileInputStream(new File("/Users/tkreutzer/Documents/clients/citi/hbase/cluster2/core-site.xml")) );
		failover.set("hadoop.security.authentication", "kerberos");
		failover.set("hbase.client.retries.number", "1"); //Override Default Parameters
		failover.set("hbase.client.pause", "1"); //Override Default Parameters
		
		Configuration cc = ConfigUtil.combineConfigurations(primary, failover);
		
		//System.out.println("Implementation Class?? " + cc.get(HBASE_CLIENT_CONNECTION_IMPL));
		
		
		//Even though we call the Connection Factory, it will override with the class "org.apache.hadoop.hbase.client.ConnectionMultiCluster"
		Connection connection = new ConnectionMultiCluster(cc);

		/*
		disable 'test_table'
		drop 'test_table'
		create 'test_table', 'cf1'
		
		
		get 'test_table', '0.key.          10', {COLUMN => 'cf1'}
		
		
		 */
		
		
		String tableName = "test_table";
		String familyName = "cf1";
		String colName = "C";
		int numberOfPuts = 1000;
		int milliSecondsOfWait = 200;
		Table table = connection.getTable(TableName.valueOf(tableName));
		for (int i = 1; i <= numberOfPuts; i++) {
			//Create key
			String key = (i % 10 + ".key." + StringUtils.leftPad(String.valueOf(i), 12));
			
			log.info("PUT: " + key);
			Put put = new Put(Bytes.toBytes(key));
			put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(colName), Bytes.toBytes("Value:" + i));
			table.put(put);
			
			log.info("GET");
			Get get = new Get(Bytes.toBytes(key));
			Result result = table.get(get);
			byte [] value = result.getValue(Bytes.toBytes( familyName ),Bytes.toBytes( colName ));
			log.info("Value: " + Bytes.toString(value));
			Thread.sleep(milliSecondsOfWait);
		}
		log.info("Closing Connection");
		connection.close();
		log.info(" - Connection Closed");
		System.exit(0);
	}

}
