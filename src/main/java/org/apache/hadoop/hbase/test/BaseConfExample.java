package org.apache.hadoop.hbase.test;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;

import com.cloudera.hbase.mcc.ConfigUtil;
import com.cloudera.hbase.mcc.HBaseMccConfiguration;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;


//truncate 'test_table'


public class BaseConfExample {
	static final Log log = LogFactory.getLog(BaseConfExample.class);
	
	public static void main(String[] args) throws Exception {
		String HBASE_CLIENT_CONNECTION_IMPL = "hbase.client.connection.impl"; //This constant seems to be moving around in versions of HBASE for now just use hard coded
		String CONNECTION_IMPL = "com.cloudera.hbase.mcc.ConnectionMultiCluster";
		
		
		HBaseMccConfiguration mccConf = new HBaseMccConfiguration();
		//Set base configuration here for the override or any parameters for MCC.
		mccConf.set(HBASE_CLIENT_CONNECTION_IMPL,CONNECTION_IMPL);
		
		//Create an HBase configuration for each cluster
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
		
		//Add each of them to the HBaseMccConfiguration, these will be prefixed for each unique cluster
		//and merged into a large conf.
		mccConf.addClusterConfig(primary);
		mccConf.addClusterConfig(failover);
		
		ArrayList<Configuration> splitconf = ConfigUtil.splitMultiConfigFile(mccConf.getConfiguration());
		
		System.out.println("Total Configs: " + splitconf.size() + "\n");
		
		System.out.println("hbase.mcc.cluster0.fs.defaultFS = " + splitconf.get(0).get("fs.defaultFS"));
		System.out.println("hbase.mcc.cluster1.fs.defaultFS = " + splitconf.get(1).get("fs.defaultFS"));
		
		System.out.println("hbase.mcc.cluster0.hbase.zookeeper.quorum = " + splitconf.get(0).get("hbase.zookeeper.quorum"));
		System.out.println("hbase.mcc.cluster1.hbase.zookeeper.quorum = " + splitconf.get(1).get("hbase.zookeeper.quorum"));
		
		
		System.out.println("Implementation Class?? " + mccConf.getConfiguration().get(HBASE_CLIENT_CONNECTION_IMPL));
		
		//Even though we call the Connection Factory, it will override with the class "org.apache.hadoop.hbase.client.ConnectionMultiCluster"
		Connection connection = ConnectionFactory.createConnection(mccConf.getConfiguration());
		
		String tableName = "test_table";
		String familyName = "cf1";
		int numberOfPuts = 1000;
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
