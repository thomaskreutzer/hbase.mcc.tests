package org.apache.hadoop.hbase.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import com.cloudera.hbase.mcc.MultiClusterConf;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import com.cloudera.hbase.mcc.ConfigUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import com.cloudera.hbase.mcc.ConnectionFactoryMultiClusterWrapper;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import com.cloudera.hbase.mcc.TableMultiCluster;
import com.cloudera.hbase.mcc.TableStats;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

public class MultiClusterUgiTest {
	static final Log log = LogFactory.getLog(MultiClusterUgiTest.class);
	/*
	Example Command:
	disable 'test_table'
	drop 'test_table'
	create 'test_table', 'cf1'
	
	java -cp hbase.multicluster-0.2.0-SNAPSHOT.jar org.apache.hadoop.hbase.test.RunMultiClusterKerberosTest \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1/hdfs-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1/core-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1/hbase-site.xml \
	cluster1tls/ccycloud-1.tkhbasetls1.root.hwx.site@TLS1.COM \
	/Users/tkreutzer/Documents/testingkeytabs/hbase/cluster1tls_host.keytab \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster2/hdfs-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster2/core-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster2/hbase-site.xml \
	cluster2tls/ccycloud-1.tkhbasetls2.root.hwx.site@TLS2.COM \
	/Users/tkreutzer/Documents/testingkeytabs/hbase/cluster2tls_host.keytab \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster3/hdfs-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster3/core-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster3/hbase-site.xml \
	cluster3tls/ccycloud-1.tkhbasetls3.root.hwx.site@TLS3.COM \
	/Users/tkreutzer/Documents/testingkeytabs/hbase/cluster3tls_host.keytab \
	test_table \
	cf1 \
	100 \
	100 \
	stats.csv
	
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1/hdfs-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1/core-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1/hbase-site.xml cluster1tls/ccycloud-1.tkhbasetls1.root.hwx.site@TLS1.COM /Users/tkreutzer/Documents/testingkeytabs/hbase/cluster1tls_host.keytab /Users/tkreutzer/git/hbase.mcc/configexamples/cluster2/hdfs-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster2/core-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster2/hbase-site.xml cluster2tls/ccycloud-1.tkhbasetls2.root.hwx.site@TLS2.COM /Users/tkreutzer/Documents/testingkeytabs/hbase/cluster2tls_host.keytab /Users/tkreutzer/git/hbase.mcc/configexamples/cluster3/hdfs-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster3/core-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster3/hbase-site.xml cluster3tls/ccycloud-1.tkhbasetls3.root.hwx.site@TLS3.COM /Users/tkreutzer/Documents/testingkeytabs/hbase/cluster3tls_host.keytab test_table cf1 100 100 stats.csv
	*/
	public static void main(String[] args) throws Exception {
		String hdfsSite1 = args[0]; 
		String coreSite1 = args[1]; 
		String hbaseSite1 = args[2];
		String principal1 = args[3];
		String keytab1 = args[4];
		
		String hdfsSite2 = args[5]; 
		String coreSite2 = args[6]; 
		String hbaseSite2 = args[7];
		String principal2 = args[8];
		String keytab2 = args[9];
		
		String hdfsSite3 = args[10]; 
		String coreSite3 = args[11]; 
		String hbaseSite3 = args[12];
		String principal3 = args[13];
		String keytab3 = args[14];
		
		
		MultiClusterConf multiClusterConf = new MultiClusterConf();
		multiClusterConf.addClusterConfig(hdfsSite1,coreSite1,hbaseSite1,principal1,keytab1); //Cluster 0
		multiClusterConf.addClusterConfig(hdfsSite2,coreSite2,hbaseSite2,principal2,keytab2); //Cluster 1
		multiClusterConf.addClusterConfig(hdfsSite3,coreSite3,hbaseSite3,principal3,keytab3); //Cluster 2
		
		multiClusterConf.set("hbase.client.retries.number", "1", 0); //Override Default Parameters
		multiClusterConf.set("hbase.client.pause", "1", 0, true); //Override Default Parameters
		
		String tableName = args[15];
		String familyName = args[16];
		int numberOfPuts = Integer.parseInt(args[17]);
		int milliSecondsOfWait = Integer.parseInt(args[18]);
		String outputCsvFile = args[19];
		
		
		
		//Connection and table
		Connection connection = ConnectionFactoryMultiClusterWrapper.createConnectionMultiUgi(multiClusterConf);
		
		//ArrayList<Connection> connections = ConnectionFactoryMultiClusterWrapper.createConnectionMultiUgiTest(multiClusterConf);
		//Connection connection = connections.get(0);
		
		
		//Connection connection = getUgiConnection(args[1],args[0],args[2],args[3],args[4]);
		
		//log.info("Getting Admin");
		//Admin hbaseAdmin = connection.getAdmin();
		//log.info(" -- Got Admin");
		
		/*log.info("Disable and delete table if exists");
		try {
			hbaseAdmin.disableTable(TableName.valueOf(tableName));
			hbaseAdmin.deleteTable(TableName.valueOf(tableName));
		} catch (Exception e) {
			e.printStackTrace();
		}
		log.info("Disable and delete table if exists -- Completed");
		
		
		
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
		log.info("  -- Created");*/
		
		
		log.info("Getting Table");
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		BufferedWriter writer = new BufferedWriter(new FileWriter(outputCsvFile));

		TableStats.printCSVHeaders(writer);

		for (int i = 1; i <= numberOfPuts; i++) {
			log.info("PUT");
			Put put = new Put(Bytes.toBytes(i % 10 + ".key." + StringUtils.leftPad(String.valueOf(i), 12)));
			put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("C"), Bytes.toBytes("Value:" + i));
			table.put(put);

			/*log.info("GET");
			Get get = new Get(Bytes.toBytes(i % 10 + ".key." + StringUtils.leftPad(String.valueOf(i), 12)));
			table.get(get);
			
			Result result = table.get(get);
			byte [] value = result.getValue(Bytes.toBytes(familyName),Bytes.toBytes("C"));
			log.info("Value of get: " + Bytes.toString(value));

			log.info("DELETE");
			Delete delete = new Delete(Bytes.toBytes(i % 10 + ".key." + StringUtils.leftPad(String.valueOf(i), 12)));
			table.delete(delete);

			System.out.print(".");
			if (i % 100 == 0) {
				System.out.println("|");
				TableStats stats = ((TableMultiCluster) table).getStats();
				stats.printPrettyStats();
				stats.printCSVStats(writer);
			}*/
			// milliSecondsOfWait
			Thread.sleep(milliSecondsOfWait);
		}

		writer.close();

		//Wrapping this in an additional try/catch so that if it happens at the end of my code and hbase is down it does not fail out the program. 
		/*try {
			hbaseAdmin.disableTable(TableName.valueOf(tableName));
			hbaseAdmin.deleteTable(TableName.valueOf(tableName));
		} catch (Exception e) {
			e.printStackTrace();
		}*/

		log.info("Closing Connection");
		connection.close();
		log.info(" - Connection Closed");
		
		//log.info("Closing Admin");
		//hbaseAdmin.close();
		//log.info(" - Admin Closed");
		
		System.exit(0);
	}
	
	
	public static Connection getUgiConnection(
			String core,
			String hdfs,
			String hbase,
			String principal,
			String keytab
	) throws IOException, InterruptedException {
		final Configuration hbaseconf = HBaseConfiguration.create();
		log.info("Core Site: " + core);
		log.info("Hdfs Site: " + hdfs);
		log.info("Hbase Site: " + hbase);
		log.info("Principal: " + principal);
		log.info("Keytab: " + keytab);
		hbaseconf.addResource(new FileInputStream(new File( core )));
		hbaseconf.addResource(new FileInputStream(new File( hdfs )));
		hbaseconf.addResource(new FileInputStream(new File( hbase )));
		UserGroupInformation.setConfiguration(hbaseconf);
		UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
				principal, 
				keytab
			);
		Connection conn = ugi.doAs(new PrivilegedExceptionAction<Connection>() {
			@Override
			public Connection run() throws Exception {
				return ConnectionFactory.createConnection(hbaseconf);
			}
		});
		return conn;
	}

}
