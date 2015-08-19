/**
 * Created by cott on 8/12/15.
 */

import backtype.storm.generated.*;
import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import backtype.storm.utils.NimbusClient;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.*;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

public class zkCliTest {


        // The callback for watching an abritrary ZooKeeper Node
        Watcher callback = new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getPath());
                System.out.println(watchedEvent.toString());
            }
        };

    // A shorthand debugging method.
    private static void print(String blah) {
        System.out.println(blah);
    }

    // The Nimbus Global Status Model #ngsm
    private static Map<String, String> AllMetrics = new HashMap<String, String>();
    

    // TCP Stream to carbon-cache.
    private void graphite(Map<String, String> metrics) {
        // Current Time-Stamp for test
	long epoch = System.currentTimeMillis()/1000;
	
        try { // output  stream to the host on default port.
        Socket conn          = new Socket("cabon-cache.novalocal", 2003);
        DataOutputStream dos = new DataOutputStream(conn.getOutputStream());
	    // graphite syntax map the #ngsm to output stream.
            for  (String metric : metrics.keySet() ){
                dos.writeBytes(metric + 
			" " + metrics.get(metric) + 
			" " + epoch + "\n");
            }
	    //CLOSED CONNECTION.
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

// TODO
//The next two methods need review and re-implementation.

	//Slow, Messy mapping of some BoltStats #bs to graphite input set. Not used at the moment.
    private void addBoltStats(BoltStats bs, String metric_path) {
        Iterator<Map.Entry<GlobalStreamId, Long>> streamIter = bs.get_executed().get(":all-time").entrySet().iterator();

        Map.Entry<GlobalStreamId, Long> val;

        while (streamIter.hasNext()) {
            val = (Map.Entry<GlobalStreamId, Long>) streamIter.next();
            if(!val.getKey().get_componentId().equals("__system")) {
                print(metric_path + "." + val.getKey().get_componentId() + "." + val.getKey().get_streamId() + ", " + val.getValue());
                this.AllMetrics.put(metric_path + "." + val.getKey().get_componentId() + "." + val.getKey().get_streamId(), val.getValue().toString());
            }
        }
    }

	//Slow, Messy mapping of some SpoutStats #ss to graphite input set. Not used at the moment.
    private void addSpoutStats(SpoutStats ss, String metric_path) {

        Set<Map.Entry<String, Map<String, Long>>> acked = ss.get_acked().entrySet();
        Iterator ackedIter = acked.iterator();

        Map.Entry<String, Map<String, Long>> val;

        while (ackedIter.hasNext()) {
            val = (Map.Entry<String, Map<String, Long>>) ackedIter.next();
            if (val.getValue().size() == 0) {
                continue;
            }
            print(metric_path + "." + val.getKey() + ", " + val.getValue());
            this.AllMetrics.put(metric_path + "." + val.getKey(), val.getValue().toString());

        }
    }

	//1. Connect to Storm Nimbus
	//2. Iterate the Bolt and Spout stats for each executor.
	//3. Add Stats to #ngsm
	//4. Write out to RRDs via carbon-cache
	//Essentially the Main method for test Singleton.
	
    public void nimbuscli() {


        try {
            Config conf = new Config();
            conf.put(Config.NIMBUS_HOST, "dresman");
            //This is likely slower, so set to False for speed.
	    conf.setDebug(true);
            Map storm_conf = Utils.readStormConfig();
            storm_conf.put("nimbus.host", "dresman");
            NimbusClient nimbus = new NimbusClient(storm_conf, "dresman", 6627);
	    
	    //Store a list of Topologies this nimbus contains.
            List<TopologySummary> topos = nimbus.getClient().getClusterInfo().get_topologies();
	    //Metric Path is updated periodically to breadcrumb the topology model
	    String mp;
            String tmp;

	    for ( TopologySummary ts : topos ) {
                /* print("==========Topology Summary===================");
                for (TopologySummary._Fields field : TopologySummary._Fields.values()) {
                    print("--" +field.getFieldName() + ": " + ts.getFieldValue(field).toString());
                } */
                //Topology Info 
                TopologyInfo ti = nimbus.getClient().getTopologyInfo(ts.get_id());
		tmp = ti.get_name();
		String fn; //Root level fields of topology
                for (TopologyInfo._Fields ifield : TopologyInfo._Fields.values()) {
		    fn = ifield.getFieldName();
                    if (fn.equals("errors")) {
			AllMetrics.put(tmp + ".error_count", Integer.toString(ti.get_errors().size()));
                        continue; 
                    }
		    // Excutors are threads.  Topologies can have a lot. More big data stuff.
                    if (fn.equals("executors")) {

                        List<ExecutorSummary> ls = (List<ExecutorSummary>) ti.getFieldValue(ifield);
                        AllMetrics.put(tmp + ".thread_count", Integer.toString(ti.get_executors().size()));

			//Threads have tasks, and tasks have stats about time and tuples...
                        ExecutorStats exec_stats;

                        Map<String, Map<String, Long>> emitted;
                        Map<String, Map<String, Long>> transferred;

                        //For each thread

                        for (int j = 0; j < ls.size(); j++) {
                            exec_stats = ls.get(j).get_stats();

                            // emitted = stats.get_emitted();
                            //  transferred = stats.get_transferred();
			    //Add thread id crumb.
                            mp = tmp +
				".thread-" + j + "-" +
				ls.get(j).get_component_id();

                            if (exec_stats.get_specific().is_set_bolt()) {
                                BoltStats bs = exec_stats.get_specific().get_bolt();
				AllMetrics.put(mp + ".type", "bolt");
                                //addBoltStats(bs, mp);
			    }

                            if (exec_stats.get_specific().is_set_spout()) {
                                SpoutStats ss = exec_stats.get_specific().get_spout();
				AllMetrics.put(mp + ".type", "spout");
                                //addSpoutStats(ss, mp);
			    }

			    // iterating the all-time emitted for each thread
                            Iterator emittedIter = exec_stats.get_emitted().get(":all-time").entrySet().iterator();
                                    while (emittedIter.hasNext()) {
                                        Map.Entry<String, Long> val = (Map.Entry<String, Long>) emittedIter.next();

                                        if (!val.getKey().startsWith("__")) {
                                            print(mp + "." + val.getKey() + ", " + val.getValue());
                                            AllMetrics.put(mp + "." + val.getKey(), val.getValue().toString());
                                        }

                                    }

                        }

                }

            }
        }

            graphite(AllMetrics);

        }catch(Exception err){
            err.printStackTrace();
        }



    }

    public void curator(String path) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("dzoo1:2181,dzoo2:2181,dzoo3:2181", retryPolicy);
        client.start();

        try {

            int datasize = client.getData().forPath(path).length;
            System.out.println("Size of data at '" + path + "' is, " + datasize);
        } catch (Exception err) {
            err.printStackTrace();
        }

    }


    public void zknative(String path)
    {


        try {
            ZooKeeper client = new ZooKeeper("dzoo1:2181,dzoo2:2181,dzoo3:2181", 300, callback);

            List<String> children = client.getChildren(path, false);

            for (int i = 0; i < children.size(); i++) {
                System.out.println(children.get(i));
            }


        } catch (Exception err) {
            err.printStackTrace();

        }


    }
}
