package org.apache.hadoop.hbase.test;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.cloudera.hbase.mcc.MultiClusterConf;

public class ConfigTest {
	static final Log log = LogFactory.getLog(ConfigTest.class);
	
	public static void main(String[] args) throws Exception {
		
		String hdfs1 = "/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1/hdfs-site.xml";
		String core1 = "/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1/core-site.xml";
		String hbase1 = "/Users/tkreutzer/git/hbase.mcc/configexamples/cluster1/hbase-site.xml";
		String principal1 = "cluster1tls/ccycloud-1.tkhbasetls1.root.hwx.site@TLS1.COM";
		String keytab1 = "/Users/tkreutzer/Documents/testingkeytabs/hbase/cluster1tls_host.keytab";
		
		String hdfs2 ="/Users/tkreutzer/git/hbase.mcc/configexamples/cluster2/hdfs-site.xml";
		String core2 = "/Users/tkreutzer/git/hbase.mcc/configexamples/cluster2/core-site.xml";
		String hbase2 = "/Users/tkreutzer/git/hbase.mcc/configexamples/cluster2/hbase-site.xml";
		String principal2 = "cluster2tls/ccycloud-1.tkhbasetls2.root.hwx.site@TLS2.COM";
		String keytab2 = "/Users/tkreutzer/Documents/testingkeytabs/hbase/cluster2_host.keytab";
		
		String hdfs3 ="/Users/tkreutzer/git/hbase.mcc/configexamples/cluster3/hdfs-site.xml";
		String core3 = "/Users/tkreutzer/git/hbase.mcc/configexamples/cluster3/core-site.xml";
		String hbase3 = "/Users/tkreutzer/git/hbase.mcc/configexamples/cluster3/hbase-site.xml";
		String principal3 = "cluster3tls/ccycloud-1.tkhbasetls3.root.hwx.site@TLS3.COM";
		String keytab3 = "/Users/tkreutzer/Documents/testingkeytabs/hbase/cluster3tls_host.keytab";
		
		MultiClusterConf multiClusterConf = new MultiClusterConf();
		//ClusterConf cc = new ClusterConf();
		multiClusterConf.addClusterConfig(hdfs1,core1,hbase1,principal1,keytab1);
		multiClusterConf.addClusterConfig(hdfs2,core2,hbase2,principal2,keytab2);
		multiClusterConf.addClusterConfig(hdfs3,core3,hbase3,principal3,keytab3);
		
		log.info(multiClusterConf.getClusterConfigs().get(0).getConf());
		
		System.exit(0);
		
	}

}
