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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class hdfsTest {
	
	/*
	Example Command:
	
	java -cp hbase.multicluster-0.2.0-SNAPSHOT.jar org.apache.hadoop.hbase.test.hdfsTest \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hdfs-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/core-site.xml \
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hbase-site.xml \
	tkreutzer/ccycloud.tkcentralkdc.root.hwx.site@CENTRAL.COM \
	/Users/tkreutzer/Documents/testingkeytabs/hbase/tkreutzer_host.keytab \
	/tmp \
	
	/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hdfs-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/core-site.xml /Users/tkreutzer/git/hbase.mcc/configexamples/cluster1tls/hbase-site.xml tkreutzer/ccycloud.tkcentralkdc.root.hwx.site@CENTRAL.COM /Users/tkreutzer/Documents/testingkeytabs/hbase/tkreutzer_host.keytab /tmp
	
	*/
	public static void main(String[] args) throws Exception {
				
		Configuration conf = new Configuration();
		conf.addResource(new FileInputStream(new File( args[0] )));
		conf.addResource(new FileInputStream(new File( args[1] )));
		conf.addResource(new FileInputStream(new File( args[2] )));
		
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromKeytab(args[3], args[4]);
		
		FileSystem fs = FileSystem.get(conf);
		
		FileStatus[] fsStatus = fs.listStatus(new Path(args[5]));
		for(int i = 0; i < fsStatus.length; i++){
			System.out.println(fsStatus[i].getPath().toString());
		}
	}
}
