/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.test;

import java.io.File;
import java.io.FileInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class HbaseScanRange {
	static final Log log = LogFactory.getLog(HbaseScanRange.class);
	
	/*
	Example Command:
	
	java -cp hbase.multicluster-0.2.0-SNAPSHOT.jar org.apache.hadoop.hbase.test.HbaseScanRange \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hdfs-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/core-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hbase-site.xml \
	tkreutzer/ccycloud.tkcentralkdc.root.hwx.site@CENTRAL.COM \
	/Users/tkreutzer/Documents/testingkeytabs/hbase/tkreutzer_host.keytab \
	test_table \
	cf1

	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hdfs-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/core-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hbase-site.xml tkreutzer/ccycloud.tkcentralkdc.root.hwx.site@CENTRAL.COM /Users/tkreutzer/Documents/testingkeytabs/hbase/tkreutzer_host.keytab test_table cf1
	*/
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new FileInputStream(new File( args[0] )));
		conf.addResource(new FileInputStream(new File( args[1] )));
		conf.addResource(new FileInputStream(new File( args[2] )));
		
		//Kerberos
		System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromKeytab(args[3],args[4]);
		
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf( args[5] ));
		
		Admin hbaseAdmin = connection.getAdmin();
		TableDescriptor tblDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf( args[5] )).setColumnFamily(ColumnFamilyDescriptorBuilder.of( args[6] )).build();
		
		
		try {
			hbaseAdmin.disableTable(TableName.valueOf( args[5] ));
			hbaseAdmin.deleteTable( TableName.valueOf( args[5] ) );
		} catch (Exception e) {
			log.error(e);
		}
		
		log.info("Creating table");
		hbaseAdmin.createTable(tblDesc);
		
		for (int i = 1; i <= 10; i++) {
			Put put = new Put(Bytes.toBytes( i ));
			put.addColumn(Bytes.toBytes( args[6] ), Bytes.toBytes("C"),Bytes.toBytes("Put number: " + i));
			log.info("Executing put: " + i);
			table.put(put);
		}
		
		Scan scan = new Scan().withStartRow(Bytes.toBytes(0)).withStopRow(Bytes.toBytes(5));
		
		ResultScanner scanner = table.getScanner(scan);
		for (Result result = scanner.next(); result != null; result = scanner.next()) {
			log.info("Found row : " + result);
		}
		
		hbaseAdmin.disableTable(TableName.valueOf( args[5] ));
		hbaseAdmin.deleteTable( TableName.valueOf( args[5] ) );
		
		table.close();
		connection.close();
		scanner.close();
		System.exit(0);
	}
}
