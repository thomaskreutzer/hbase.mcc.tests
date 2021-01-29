package org.apache.hadoop.hbase.test;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;
import com.cloudera.hbase.mcc.*;

public class CombineSplitTest {
	static Logger log = Logger.getLogger(CombineSplitTest.class);
	public static void main(String[] args) throws Exception {
		System.out.println("Starting application\n");
		
		//Going to test a very simplified hbase-site.xml file.
		Configuration primary = HBaseConfiguration.create();
		Configuration failover = HBaseConfiguration.create();
		
		primary.addResource( new FileInputStream(new File("/Users/tkreutzer/Documents/clients/citi/hbase/configtesting/cluster1/hbase-site.xml")) );
		primary.addResource( new FileInputStream(new File("/Users/tkreutzer/Documents/clients/citi/hbase/configtesting/cluster1/core-site.xml")) );
		
		failover.addResource( new FileInputStream(new File("/Users/tkreutzer/Documents/clients/citi/hbase/configtesting/cluster2/hbase-site.xml")) );
		failover.addResource( new FileInputStream(new File("/Users/tkreutzer/Documents/clients/citi/hbase/configtesting/cluster2/core-site.xml")) );
		

		ArrayList<Configuration> confs = new ArrayList<Configuration>();
		confs.add(primary);
		confs.add(failover);
		
		Configuration combined = ConfigUtil.combineConfigurations(confs);
		
		System.out.println("hbase.mcc.cluster0.fs.defaultFS = " + combined.get("hbase.mcc.cluster0.fs.defaultFS"));
		System.out.println("hbase.mcc.cluster1.fs.defaultFS = " + combined.get("hbase.mcc.cluster1.fs.defaultFS"));
		
		System.out.println("hbase.mcc.cluster0.hbase.zookeeper.quorum = " + combined.get("hbase.mcc.cluster0.hbase.zookeeper.quorum"));
		System.out.println("hbase.mcc.cluster1.hbase.zookeeper.quorum = " + combined.get("hbase.mcc.cluster1.hbase.zookeeper.quorum"));
		
		
		System.out.println("\n\nNext looking at the split \n\n");
		
		ArrayList<Configuration> splitconf = ConfigUtil.splitMultiConfigFile(combined);
		
		System.out.println("Total Configs: " + splitconf.size() + "\n");
		
		System.out.println("hbase.mcc.cluster0.fs.defaultFS = " + splitconf.get(0).get("fs.defaultFS"));
		System.out.println("hbase.mcc.cluster1.fs.defaultFS = " + splitconf.get(1).get("fs.defaultFS"));
		
		System.out.println("hbase.mcc.cluster0.hbase.zookeeper.quorum = " + splitconf.get(0).get("hbase.zookeeper.quorum"));
		System.out.println("hbase.mcc.cluster1.hbase.zookeeper.quorum = " + splitconf.get(1).get("hbase.zookeeper.quorum"));
		
		System.out.println("\nCompleting application");
	}
}
