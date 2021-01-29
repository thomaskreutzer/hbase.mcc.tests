package org.apache.hadoop.hbase.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.cloudera.hbase.mcc.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;



public class BuilderPatternTest {
static final Log log = LogFactory.getLog(BuilderPatternTest.class);
	
	public static void main(String[] args) throws Exception {
		ClusterConfig cluster1 = new ClusterConfig.ClusterConfigBuilder()
				.hdfsSite("/Users/tkreutzer/Documents/clients/citi/hbase/cluster1/hdfs-site.xml")
				.coreSite("/Users/tkreutzer/Documents/clients/citi/hbase/cluster1/core-site.xml")
				.hbaseSite("/Users/tkreutzer/Documents/clients/citi/hbase/cluster1/hbase-site.xml")
				.principal("cluster1tls/ccycloud-1.tkhbasetls1.root.hwx.site@TLS1.COM")
				.keytab("/Users/tkreutzer/Documents/clients/citi/hbase/cluster1/cluster1tls_host.keytab")
				.build();
		
		//Set override parameters for cluster 1
		cluster1.set("hbase.client.retries.number", "1");
		cluster1.set("hbase.client.pause", "1");
		
		ClusterConfig cluster2 = new ClusterConfig.ClusterConfigBuilder()
				.hdfsSite("/Users/tkreutzer/Documents/clients/citi/hbase/cluster2/hdfs-site.xml")
				.coreSite("/Users/tkreutzer/Documents/clients/citi/hbase/cluster2/core-site.xml")
				.hbaseSite("/Users/tkreutzer/Documents/clients/citi/hbase/cluster2/hbase-site.xml")
				.principal("cluster2tls/ccycloud-1.tkhbasetls2.root.hwx.site@TLS2.COM")
				.keytab("/Users/tkreutzer/Documents/clients/citi/hbase/cluster2/cluster2tls_host.keytab")
				.build();
		
		//Set override parameters for cluster 2
		cluster2.set("hbase.client.retries.number", "1");
		cluster2.set("hbase.client.pause", "1");
		
		//Add to multi-cluster
		MultiClusterConf multiClusterConf = new MultiClusterConf();
		multiClusterConf.addClusterConfig(cluster1);
		multiClusterConf.addClusterConfig(cluster2);
		
		Connection connection = ConnectionFactoryMultiClusterWrapper.createConnectionMultiUgi(multiClusterConf);
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
