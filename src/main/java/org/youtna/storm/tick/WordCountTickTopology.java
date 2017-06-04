package org.youtna.storm.tick;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by v_wbytna on 2017/6/2.
 */
public class WordCountTickTopology {
    private static final Logger logger = LoggerFactory.getLogger(WordCountTickTopology.class);
    private static String topologyName = "wordCount-tick";
    private TopologyBuilder builder;
    private Config conf;
    private ObjectMapper mapper = new ObjectMapper();

    private Map<String, Object> params = new HashMap<String, Object>();

    public WordCountTickTopology(String args) {
        parseArgs(args);
        this.builder = new TopologyBuilder();
        builder.setSpout("word-spout",new WordSpout(),(Integer)this.params.get("spout")).setNumTasks(1);
        builder.setBolt("split-bolt",new SplitWordBolt(),(Integer)this.params.get("split")).setNumTasks(1).
                shuffleGrouping("word-spout");
        builder.setBolt("count-bolt",new CountTickBolt(),(Integer)this.params.get("count")).setNumTasks(1).
                fieldsGrouping("split-bolt",new Fields("Word"));
//        builder.setSpout("word-spout",new WordSpout(),2).setNumTasks(1);
//        builder.setBolt("split-bolt",new SplitWordBolt(),2).setNumTasks(1).
//                shuffleGrouping("word-spout");
//        builder.setBolt("count-bolt",new CountTickBolt(),2).setNumTasks(1).
//                fieldsGrouping("split-bolt",new Fields("Word"));

        this.conf = new Config();
        this.conf.setDebug(true);
        this.conf.setNumWorkers(((Integer) this.params.get("worker_num")).intValue());
        this.conf.setMessageTimeoutSecs(((Integer) this.params.get("timeout_secs")).intValue());
//        this.conf.setNumWorkers(1);
//        this.conf.setMessageTimeoutSecs(5000);
    }

    private void parseArgs(String args) {
        if (StringUtils.isEmpty(args)) {
            return;
        }
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> map = new HashMap();
        try {
            map = (Map) mapper.readValue(args, Map.class);
        } catch (JsonGenerationException e) {
            logger.error("Json parse error", e);
        } catch (JsonMappingException e) {
            logger.error("Json mapping error", e);
        } catch (IOException e) {
            logger.error("Json IO error", e);
        }
        this.params.putAll(map);
    }

    private void runLocal() {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, this.conf, this.builder.createTopology());
        Utils.sleep(((Integer) this.params.get("local_exec_time")).intValue() * 1000);
        cluster.shutdown();
    }

    private void runRemote() throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        StormSubmitter.submitTopology(topologyName, this.conf, this.builder.createTopology());
    }

    public static void main(String[] args)
            throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        boolean runLocally = true;
        if ((args.length >= 1) && (args[0].equalsIgnoreCase("remote"))) {
            runLocally = false;
        }
        if (args.length >= 2) {
            topologyName = StringUtils.isEmpty(args[1]) ? topologyName : args[1];
        }
        WordCountTickTopology topo = new WordCountTickTopology(args.length >= 3 ? args[2] : null);
        if (runLocally) {
            logger.info("Running in local mode");
            topo.runLocal();
        } else {
            logger.info("Running in remote (cluster) mode");
            topo.runRemote();
        }
    }
}
