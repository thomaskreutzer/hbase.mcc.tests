package org.apache.hadoop.hbase.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.cloudera.hbase.mcc.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;


// mvn install:install-file -Dfile=/Users/tkreutzer/git/hbase.mcc/target/hbase-mcc-0.2.0-SNAPSHOT.jar -DgroupId=com.cloudera.hbasemcc -DartifactId=hbase-mcc -Dversion=0.2.0-SNAPSHOT -Dpackaging=jar



public class BuilderPatternTest2 {
static final Log log = LogFactory.getLog(BuilderPatternTest2.class);
	
	public static void main(String[] args) throws Exception {
		ClusterConfig cluster1 = new ClusterConfig.ClusterConfigBuilder()
				.hdfsSite(args[0])
				.coreSite(args[1])
				.hbaseSite(args[2])
				.principal(args[3])
				.keytab(args[4])
				.build();
		
		//Set override parameters for cluster 1
		cluster1.set("hbase.client.retries.number", "1");
		cluster1.set("hbase.client.pause", "1");
		
		ClusterConfig cluster2 = new ClusterConfig.ClusterConfigBuilder()
				.hdfsSite(args[5])
				.coreSite(args[6])
				.hbaseSite(args[7])
				.principal(args[8])
				.keytab(args[9])
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
