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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import com.cloudera.hbase.mcc.*;

public class MultiClusterHbaseAdminCreateTableKerberos {
	static final Log log = LogFactory.getLog(MultiClusterHbaseAdminCreateTableKerberos.class);

	/*
	Example Command:
	
	java -cp hbase.multicluster-0.2.0-SNAPSHOT.jar org.apache.hadoop.hbase.test.MultiClusterHbaseAdminCreateTableKerberos \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hdfs-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/core-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hbase-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster2tls/hdfs-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster2tls/core-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster2tls/hbase-site.xml \
	tkreutzer/ccycloud.tkcentralkdc.root.hwx.site@CENTRAL.COM \
	/Users/tkreutzer/Documents/testingkeytabs/hbase/tkreutzer_host.keytab \
	test_table \
	cf1

	
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hdfs-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/core-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hbase-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster2tls/hdfs-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster2tls/core-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster2tls/hbase-site.xml tkreutzer/ccycloud.tkcentralkdc.root.hwx.site@CENTRAL.COM /Users/tkreutzer/Documents/testingkeytabs/hbase/tkreutzer_host.keytab test_table cf1
	
	*/
	public static void main(String[] args) throws Exception {
		MultiClusterConf multiClusterConf = new MultiClusterConf();
		multiClusterConf.addClusterConfig(args[0],args[1],args[2],args[6],args[7]);
		multiClusterConf.addClusterConfig(args[3],args[4],args[5],args[6],args[7]);
		/*multiClusterConf.set("hbase.client.retries.number", "1");
		multiClusterConf.set("hbase.client.pause", "1");*/
		
		String tableName = args[8];
		String familyName = args[9];
		
		//Connect with multi-cluster
		Connection connection = ConnectionFactoryMultiClusterWrapper.createConnectionMultiUgi(multiClusterConf);
		Admin hbaseAdmin = connection.getAdmin();
		TableDescriptor tblDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName)).setColumnFamily(ColumnFamilyDescriptorBuilder.of(familyName)).build();
		
		
		//Attempt to disable the table before create
		try {
			log.info("Disable and delete table if it exists");
			hbaseAdmin.disableTable(TableName.valueOf(tableName));
			hbaseAdmin.deleteTable(TableName.valueOf(tableName));
		} catch (Exception e) {
			log.error(e);
		}
		
		hbaseAdmin.createTable(tblDesc);
		log.info("Tables created with success");
		
		hbaseAdmin.disableTable(TableName.valueOf(tableName));
		log.info("Tables disabled with success");
		
		hbaseAdmin.deleteTable( TableName.valueOf(tableName) );
		log.info("Tables deleted with success");
		
		connection.close();
		System.exit(0);
	}
	
}
