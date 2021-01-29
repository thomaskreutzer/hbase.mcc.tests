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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MultiClusterPut {
	static final Log log = LogFactory.getLog(MultiClusterPut.class);

	/*
	This test expects the target table to exist in all target clusters prior to execution of the test.
	disable 'test_table'
	drop 'test_table'
	create 'test_table', 'cf1'
	
	Example Command:

	java -cp hbase.multicluster-0.2.0-SNAPSHOT.jar org.apache.hadoop.hbase.test.MultiClusterPut \
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
	testky1 \ #key
	multiclustertest \ #value
	
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1/hdfs-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1/core-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1/hbase-site.xml cluster1tls/ccycloud-1.tkhbasetls1.root.hwx.site@TLS1.COM /Users/tkreutzer/Documents/testingkeytabs/hbase/cluster1tls_host.keytab /Users/tkreutzer/git/hbase.mcc/configexamples/cluster2/hdfs-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster2/core-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster2/hbase-site.xml cluster2tls/ccycloud-1.tkhbasetls2.root.hwx.site@TLS2.COM /Users/tkreutzer/Documents/testingkeytabs/hbase/cluster2tls_host.keytab /Users/tkreutzer/git/hbase.mcc/configexamples/cluster3/hdfs-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster3/core-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster3/hbase-site.xml cluster3tls/ccycloud-1.tkhbasetls3.root.hwx.site@TLS3.COM /Users/tkreutzer/Documents/testingkeytabs/hbase/cluster3tls_host.keytab test_table cf1 testky1 multiclustertest
	
	*/
	
	public static void main(String[] args) throws Exception {
		//Set up the resources
		MultiClusterConf multiClusterConf = new MultiClusterConf();
		multiClusterConf.addClusterConfig(args[0],args[1],args[2],args[3],args[4]);
		multiClusterConf.addClusterConfig(args[5],args[6],args[7],args[8],args[9]);
		multiClusterConf.addClusterConfig(args[10],args[11],args[12],args[13],args[14]);
		multiClusterConf.set("hbase.client.retries.number", "1", 0);
		multiClusterConf.set("hbase.client.pause", "1", 0);
		multiClusterConf.set("hbase.mcc.failover.mode", "true", 0);
		multiClusterConf.set("hbase.mcc.speculative.mutator", "false", 0, true);
		
		
		String tableName = args[15];
		String familyName = args[16];
		
		
		//Connection and table
		Connection connection = ConnectionFactoryMultiClusterWrapper.createConnectionMultiUgi(multiClusterConf);
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		
		log.info("Executing single put");
		try {
			Put put = new Put(Bytes.toBytes(args[10]));
			put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("C"),Bytes.toBytes(args[11]));
			log.info("Putting data: ");
			table.put(put);
			log.info("Put Completed: ");
		} catch (Exception e) {
			log.error(e);
		} finally {
			table.close();
			log.info("Closed Table: " + tableName);
			connection.close();
			log.info("Closed Connection");
		}
		
		log.info("Application Completed");
		System.exit(0);
		
	}
	
	
}
