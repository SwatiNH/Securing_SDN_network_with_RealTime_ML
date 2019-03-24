import kafka.api.OffsetRequest;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.UUID;

    public class KafkaStormSample {
        public static void main(String[] args) throws Exception{
            Config config = new Config();
            config.setDebug(true);
            config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
            String zkConnString = "localhost:2181";
            String topic = "network-topic";
            BrokerHosts hosts = new ZkHosts(zkConnString);

            SpoutConfig kafkaSpoutConfig = new SpoutConfig (hosts, topic, "/" + topic,
                    UUID.randomUUID().toString());
            kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
            kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
            kafkaSpoutConfig.startOffsetTime = OffsetRequest.EarliestTime();
            kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig));
            //builder.setBolt("total-packet-count-spitter", new TotalPackets()).shuffleGrouping("kafka-spout");
            //builder.setBolt("avg-inflow-packet-count-spitter", new Average_Inflow_Packet_Length()).shuffleGrouping("kafka-spout");
            builder.setBolt("avg-number-of-inflow-packets-spitter", new Average_Number_Of_Inflow_Packets()).shuffleGrouping("kafka-spout");
            //builder.setBolt("word-counter", new CountBolt()).shuffleGrouping("word-spitter");

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("KafkaStormSample", config, builder.createTopology());

            //Thread.sleep(10000);

            //cluster.shutdown();
        }
    }
