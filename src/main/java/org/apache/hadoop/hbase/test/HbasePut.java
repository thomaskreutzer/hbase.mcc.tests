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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import com.cloudera.hbase.mcc.ConfigConst;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Random;
import org.apache.commons.lang.StringUtils;
import java.io.File;
import java.io.FileInputStream;

public class HbasePut {
	static final Log log = LogFactory.getLog(HbasePut.class);

	/*
	Example Command:
	
	java -cp hbase.multicluster-0.2.0-SNAPSHOT.jar org.apache.hadoop.hbase.test.HbasePut \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1/hdfs-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1/core-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1/hbase-site.xml \
	cluster1tls/ccycloud-1.tkhbasetls1.root.hwx.site@TLS1.COM \
	/Users/tkreutzer/Documents/testingkeytabs/hbase/cluster1tls_host.keytab \
	test_table \
	cf1 \
	10 \
	10 \
	
	
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1/hdfs-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1/core-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1/hbase-site.xml cluster1tls/ccycloud-1.tkhbasetls1.root.hwx.site@TLS1.COM /Users/tkreutzer/Documents/testingkeytabs/hbase/cluster1tls_host.keytab test_table cf1 10 10
	
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
		int numberOfPuts = Integer. parseInt(args[7]);
		int secondsOfWait = Integer. parseInt(args[8]);
		
		log.info("tableName: " + tableName);
		log.info("familyName: " + familyName);
		log.info("secondsOfWait: " + secondsOfWait);
		log.info("numberOfPuts: " + numberOfPuts);
		log.info("secondsOfWait: " + secondsOfWait);
		
		log.info(ConfigConst.HBASE_MCC_FAILOVER_CLUSTERS + ": "+ conf.get(ConfigConst.HBASE_MCC_FAILOVER_CLUSTERS));
		log.info("hbase.zookeeper.quorum: " + conf.get("hbase.zookeeper.quorum"));
		log.info("hbase.failover.cluster.fail1.hbase.hstore.compaction.max: " + conf.get("hbase.failover.cluster.fail1.hbase.hstore.compaction.max"));

		log.info("Getting Table");
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		log.info("Executing puts");
		try {
			Random r = new Random();
			for (int i = 1; i <= numberOfPuts; i++) {
				int hash = r.nextInt(10);
				Put put = new Put(Bytes.toBytes(hash + ".key." + i + "." + StringUtils.leftPad(String.valueOf(i * 1), 12)));
				put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("C"),Bytes.toBytes("Value:" + i * 1));
				log.info("Putting data: " + i);
				table.put(put);
				log.info("Put Completed: " + i);
				Thread.sleep(secondsOfWait);
			}
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
