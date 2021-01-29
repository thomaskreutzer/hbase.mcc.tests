package org.apache.hadoop.hbase.test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import org.apache.commons.lang.StringUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import com.cloudera.hbase.mcc.*;

public class RunMultiClusterKerberosTest {
	static final Log log = LogFactory.getLog(RunMultiClusterKerberosTest.class);
	/*
	Example Command:
	
	java -cp hbase.multicluster-0.2.0-SNAPSHOT.jar org.apache.hadoop.hbase.test.RunMultiClusterKerberosTest \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hdfs-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/core-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hbase-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster2tls/hdfs-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster2tls/core-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster2tls/hbase-site.xml \
	tkreutzer/ccycloud.tkcentralkdc.root.hwx.site@CENTRAL.COM \
	/Users/tkreutzer/Documents/testingkeytabs/hbase/tkreutzer_host.keytab \
	test_table \
	cf1 \
	100 \
	100 \
	stats.csv
	
	Run with cluster 1 as primary
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hdfs-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/core-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hbase-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster2tls/hdfs-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster2tls/core-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster2tls/hbase-site.xml tkreutzer/ccycloud.tkcentralkdc.root.hwx.site@CENTRAL.COM /Users/tkreutzer/Documents/testingkeytabs/hbase/tkreutzer_host.keytab test_table cf1 100 100 stats.csv
	
	Run with cluster 2 as primary
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster2tls/hdfs-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster2tls/core-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster2tls/hbase-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hdfs-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/core-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hbase-site.xml tkreutzer/ccycloud.tkcentralkdc.root.hwx.site@CENTRAL.COM /Users/tkreutzer/Documents/testingkeytabs/hbase/tkreutzer_host.keytab test_table cf1 100 100 stats.csv
	
	*/
	public static void main(String[] args) throws Exception {
		MultiClusterConf multiClusterConf = new MultiClusterConf();
		multiClusterConf.addClusterConfig(args[0],args[1],args[2],args[6],args[7]);
		multiClusterConf.addClusterConfig(args[3],args[4],args[5],args[6],args[7]);
		
		String tableName = args[8];
		String familyName = args[9];
		int numberOfPuts = Integer.parseInt(args[10]);
		int milliSecondsOfWait = Integer.parseInt(args[11]);
		String outputCsvFile = args[12];
		

		
		log.info("Getting Multi-Cluster Connection");
		Connection connection = ConnectionFactoryMultiClusterWrapper.createConnectionMultiUgi(multiClusterConf);
		log.info(" -- Got Connection");
		
		log.info("Getting Admin");
		Admin hbaseAdmin = connection.getAdmin();
		log.info(" -- Got Admin");
		
		try {
			hbaseAdmin.disableTable(TableName.valueOf(tableName));
			hbaseAdmin.deleteTable(TableName.valueOf(tableName));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		log.info("Creating Table");
		TableDescriptor tblDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName)).setColumnFamily(ColumnFamilyDescriptorBuilder.of(familyName)).build();
		byte[][] splitKeys = new byte[10][1];
		splitKeys[0][0] = '0';
		splitKeys[1][0] = '1';
		splitKeys[2][0] = '2';
		splitKeys[3][0] = '3';
		splitKeys[4][0] = '4';
		splitKeys[5][0] = '5';
		splitKeys[6][0] = '6';
		splitKeys[7][0] = '7';
		splitKeys[8][0] = '8';
		splitKeys[9][0] = '9';
		hbaseAdmin.createTable(tblDesc, splitKeys);
		log.info("  -- Created");
		
		
		log.info("Getting Table");
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		BufferedWriter writer = new BufferedWriter(new FileWriter(outputCsvFile));

		TableStats.printCSVHeaders(writer);

		for (int i = 1; i <= numberOfPuts; i++) {
			log.info("PUT");
			Put put = new Put(Bytes.toBytes(i % 10 + ".key." + StringUtils.leftPad(String.valueOf(i), 12)));
			put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("C"), Bytes.toBytes("Value:" + i));
			table.put(put);

			//log.info("GET");
			//Get get = new Get(Bytes.toBytes(i % 10 + ".key." + StringUtils.leftPad(String.valueOf(i), 12)));
			//table.get(get);
			
			/*Result result = table.get(get);
			byte [] value = result.getValue(Bytes.toBytes(familyName),Bytes.toBytes("C"));
			log.info("Value of get: " + Bytes.toString(value));*/

			/*log.info("DELETE");
			Delete delete = new Delete(Bytes.toBytes(i % 10 + ".key." + StringUtils.leftPad(String.valueOf(i), 12)));
			table.delete(delete);*/

			System.out.print(".");
			if (i % 100 == 0) {
				System.out.println("|");
				TableStats stats = ((TableMultiCluster) table).getStats();
				stats.printPrettyStats();
				stats.printCSVStats(writer);
			}
			// milliSecondsOfWait
			Thread.sleep(milliSecondsOfWait);
		}

		writer.close();

		//Wrapping this in an additional try/catch so that if it happens at the end of my code and hbase is down it does not fail out the program. 
		try {
			hbaseAdmin.disableTable(TableName.valueOf(tableName));
			hbaseAdmin.deleteTable(TableName.valueOf(tableName));
		} catch (Exception e) {
			e.printStackTrace();
		}

		log.info("Closing Connection");
		connection.close();
		log.info(" - Connection Closed");
		
		log.info("Closing Admin");
		hbaseAdmin.close();
		log.info(" - Admin Closed");
		
		System.exit(0);
	}

}
