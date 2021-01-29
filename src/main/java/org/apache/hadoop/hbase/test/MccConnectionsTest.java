package org.apache.hadoop.hbase.test;

import java.io.File;
import java.io.FileInputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import com.cloudera.hbase.mcc.ConfigUtil;
import com.cloudera.hbase.mcc.ConnectionMultiCluster;

public class MccConnectionsTest {
	static Logger log = Logger.getLogger(MccConnectionsTest.class);
	public static void main(String[] args) throws Exception {
		Configuration primary = HBaseConfiguration.create();
		primary.addResource( new FileInputStream(new File("/Users/tkreutzer/Documents/clients/citi/hbase/cluster1/hbase-site.xml")) );
		primary.addResource( new FileInputStream(new File("/Users/tkreutzer/Documents/clients/citi/hbase/cluster1/core-site.xml")) );
		primary.set("hadoop.security.authentication", "kerberos");
		primary.set("hbase.client.retries.number", "1"); //Override Default Parameters
		primary.set("hbase.client.pause", "1"); //Override Default Parameters
		
		UserGroupInformation.setConfiguration(primary);
		UserGroupInformation.loginUserFromKeytab("cluster1tls/ccycloud-1.tkhbasetls1.root.hwx.site@TLS1.COM","/Users/tkreutzer/Documents/clients/citi/hbase/cluster1/cluster1tls_host.keytab");
		
		Configuration failover = HBaseConfiguration.create();
		failover.addResource( new FileInputStream(new File("/Users/tkreutzer/Documents/clients/citi/hbase/cluster2/hbase-site.xml")) );
		failover.addResource( new FileInputStream(new File("/Users/tkreutzer/Documents/clients/citi/hbase/cluster2/core-site.xml")) );
		failover.set("hadoop.security.authentication", "kerberos");
		failover.set("hbase.client.retries.number", "1"); //Override Default Parameters
		failover.set("hbase.client.pause", "1"); //Override Default Parameters
		
		Configuration cc = ConfigUtil.combineConfigurations(primary, failover);
		
		Connection connection = new ConnectionMultiCluster(cc);
		
		String tableName = "test_table";
		String familyName = "cf1";
		int numberOfPuts = 10;
		int milliSecondsOfWait = 200;
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
