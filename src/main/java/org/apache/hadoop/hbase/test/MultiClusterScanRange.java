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

import com.cloudera.hbase.mcc.ConnectionFactoryMultiClusterWrapper;
import com.cloudera.hbase.mcc.MultiClusterConf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class MultiClusterScanRange {
	static final Log log = LogFactory.getLog(MultiClusterScanRange.class);
	
	/*
	This test expects the target table to exist on all clusters prior to execution of the test.
	Data must also exist
	
	hbase shell:
	
	disable table 'test_table'
	drop table 'test_table'
	create 'test_table', 'cf1'
	put 'test_table',0,'cf1:data','my data test'
	
	Example Command:
	
	java -cp hbase.multicluster-0.2.0-SNAPSHOT.jar org.apache.hadoop.hbase.test.MultiClusterScanRange \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hdfs-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/core-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hbase-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster2tls/hdfs-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster2tls/core-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster2tls/hbase-site.xml \
	tkreutzer/ccycloud.tkcentralkdc.root.hwx.site@CENTRAL.COM \
	/Users/tkreutzer/Documents/testingkeytabs/hbase/tkreutzer_host.keytab \
	test_table

	
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hdfs-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/core-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hbase-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster2tls/hdfs-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster2tls/core-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster2tls/hbase-site.xml tkreutzer/ccycloud.tkcentralkdc.root.hwx.site@CENTRAL.COM /Users/tkreutzer/Documents/testingkeytabs/hbase/tkreutzer_host.keytab test_table
	
	*/
	public static void main(String[] args) throws Exception {
		MultiClusterConf multiClusterConf = new MultiClusterConf();
		multiClusterConf.addClusterConfig(args[0],args[1],args[2],args[6],args[7]);
		multiClusterConf.addClusterConfig(args[3],args[4],args[5],args[6],args[7]);
		/*multiClusterConf.set("hbase.client.retries.number", "1");
		multiClusterConf.set("hbase.client.pause", "1");*/
		
		String tableName = args[8];
		

		//Connect with multi-cluster
		Connection connection = ConnectionFactoryMultiClusterWrapper.createConnectionMultiUgi(multiClusterConf);
		Table table = connection.getTable(TableName.valueOf(tableName));
		Scan scan = new Scan().withStartRow(Bytes.toBytes("0")).withStopRow(Bytes.toBytes("1"));
		
		ResultScanner scanner = table.getScanner(scan);
		for (Result result = scanner.next(); result != null; result = scanner.next()) {
			System.out.println("Found row : " + result);
		}
		table.close();
		connection.close();
		scanner.close();
		System.exit(0);
	}
}
