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
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.security.UserGroupInformation;

public class HbaseKerberosMultiUgiTest {
	static final Log log = LogFactory.getLog(HbaseKerberosMultiUgiTest.class);
	
	/*
	Example Command:
	
	java -cp hbase.multicluster-0.2.0-SNAPSHOT.jar org.apache.hadoop.hbase.test.HbaseKerberosMultiUgiTest \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1kerberos/hdfs-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1kerberos/core-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1kerberos/hbase-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster2kerberos/hdfs-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster2kerberos/core-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster2kerberos/hbase-site.xml \
	hbase/ccycloud-1.tkhbasekerberos1.root.hwx.site@KRB1.COM \
	/Users/tkreutzer/Documents/testingkeytabs/hbase/hbase1.keytab \
	hbase/ccycloud-1.tkhbasekerberos2.root.hwx.site@KRB2.COM \
	/Users/tkreutzer/Documents/testingkeytabs/hbase/hbase2.keytab \
	test_table \
	cf1 \

	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1kerberos/hdfs-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1kerberos/core-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1kerberos/hbase-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster2kerberos/hdfs-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster2kerberos/core-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster2kerberos/hbase-site.xml hbase/ccycloud-1.tkhbasekerberos1.root.hwx.site@KRB1.COM /Users/tkreutzer/Documents/testingkeytabs/hbase/hbase1.keytab hbase/ccycloud-1.tkhbasekerberos2.root.hwx.site@KRB2.COM /Users/tkreutzer/Documents/testingkeytabs/hbase/hbase2.keytab test_table cf1
	*/
	public static void main(String[] args) throws Exception {
		HbaseKerberosMultiUgiTest hkmut = new HbaseKerberosMultiUgiTest();

		UgiWrapper uw1 = hkmut.getUgiConnection(args[0], args[1], args[2], args[6], args[7]);
		UgiWrapper uw2 = hkmut.getUgiConnection(args[3], args[4], args[5], args[8], args[9]);
		
		createTable(uw1.getConnection(), args[10], args[11]);
		createTable(uw2.getConnection(), args[10], args[11]);
	}
	
	public static void createTable(Connection conn, String tableName, String family) throws IOException {
		Admin hbaseAdmin = conn.getAdmin();
		TableDescriptor tblDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName)).setColumnFamily(ColumnFamilyDescriptorBuilder.of(family)).build();
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
	}
	
	
	public UgiWrapper getUgiConnection(
			String hdfsSite, 
			String coreSite,
			String hbaseSite,
			String principal, 
			String keyTabPath
	) throws IOException, InterruptedException {
		final Configuration conf = HBaseConfiguration.create();
		conf.addResource(new FileInputStream(new File( hdfsSite )));
		conf.addResource(new FileInputStream(new File( coreSite )));
		conf.addResource(new FileInputStream(new File( hbaseSite )));
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keyTabPath);
		Connection conn = ugi.doAs(new PrivilegedExceptionAction<Connection>() {
			@Override
			public Connection run() throws Exception {
				return ConnectionFactory.createConnection(conf);
			}
		});
		return new UgiWrapper(conf, conn);
	}
}

class UgiWrapper {
	private Configuration conf;
	private Connection conn;
	
	UgiWrapper(Configuration conf, Connection conn) {
		this.conf = conf;
		this.conn = conn;
	}
	public Configuration getConf() {
		return conf;
	}
	public Connection getConnection() {
		return conn;
	}
}