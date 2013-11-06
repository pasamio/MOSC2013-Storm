package mosc;

import java.util.ArrayList;
import java.util.List;

import storm.kafka.KafkaConfig;
import storm.kafka.KafkaConfig.StaticHosts;
import storm.kafka.StringScheme;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Debug;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;


import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.utils.Utils;

public class MessageProcessingTopology {
	public static void main(String[] args) throws Exception {
		// Build the topology.
		List<String> hosts = new ArrayList<String>();
		hosts.add("localhost");

		TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(StaticHosts.fromHostString(hosts, 1), "mosc");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConfig.forceFromStart = false;

		TransactionalTridentKafkaSpout spout = new TransactionalTridentKafkaSpout(kafkaConfig);
		TridentTopology topology = new TridentTopology();

		Stream mainStream = topology.newStream("stream", spout);

		mainStream.each(new Fields("str"), new Debug())
			.partitionPersist(new MemoryMapState.Factory(),
				new Fields("str"),
				new MoscPersister()
		);
		
		// Set some configuration options to enable debugging.
		Config conf = new Config();
		conf.setDebug(false);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("kafka-test", conf, topology.build());
		
		Thread.sleep(600000);
	}
	
}
