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
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

public class HbaseGet {
	static final Log log = LogFactory.getLog(HbaseGet.class);
	
	/*
	This test expects the target table to exist prior to execution of the test.
	Data must also exist
	
	hbase shell:
	
	disable table 'test_table'
	drop table 'test_table'
	create 'test_table', 'cf1'
	put 'test_table',0,'cf1:data','my data test'
	
	Example Command:
	
	java -cp hbase.multicluster-0.2.0-SNAPSHOT.jar org.apache.hadoop.hbase.test.HbaseGet \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hdfs-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/core-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hbase-site.xml \
	tkreutzer/ccycloud.tkcentralkdc.root.hwx.site@CENTRAL.COM \
	/Users/tkreutzer/Documents/testingkeytabs/hbase/tkreutzer_host.keytab \
	test_table \
	cf1 \
	data

	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hdfs-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/core-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hbase-site.xml tkreutzer/ccycloud.tkcentralkdc.root.hwx.site@CENTRAL.COM /Users/tkreutzer/Documents/testingkeytabs/hbase/tkreutzer_host.keytab test_table cf1 data
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
		
		String tableName = args[5];
		String familyName = args[6];
		String colName = args[7];
		
		log.info("Getting Table");
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		Get get = new Get(Bytes.toBytes("0"));
		
		Result result = table.get(get);
		
		byte [] value = result.getValue(Bytes.toBytes( familyName ),Bytes.toBytes( colName ));
		
		log.info("Value: " + Bytes.toString(value));
		
		table.close();
		log.info("Closed Table: " + tableName);
		connection.close();
		log.info("Closed Connection");
		log.info("Application Completed");
		System.exit(0);
		
	}
}
