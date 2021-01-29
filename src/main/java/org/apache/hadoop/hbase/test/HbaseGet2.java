package org.apache.hadoop.hbase.test;

import java.io.File;
import java.io.FileInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

public class HbaseGet2 {
	static final Log log = LogFactory.getLog(HbaseGet2.class);
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource( new FileInputStream(new File("/Users/tkreutzer/Documents/clients/citi/hbase/cluster1/hbase-site.xml")) );
		conf.addResource( new FileInputStream(new File("/Users/tkreutzer/Documents/clients/citi/hbase/cluster1/core-site.xml")) );
		
		//Kerberos
		System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromKeytab("cluster1tls/ccycloud-1.tkhbasetls1.root.hwx.site@TLS1.COM","/Users/tkreutzer/Documents/clients/citi/hbase/cluster1/cluster1tls_host.keytab");
		
		Connection connection = ConnectionFactory.createConnection(conf);
		
		String tableName = "test_table";
		String familyName = "cf1";
		String colName = "C";
		
		log.info("Getting Table");
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		Get get = new Get(Bytes.toBytes("0.key.          10"));
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
