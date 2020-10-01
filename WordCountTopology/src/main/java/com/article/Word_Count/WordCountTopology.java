
package com.article.Word_Count;

import com.article.Word_Count.SentenceSplitterBolt;
import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import com.article.Word_Count.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.LocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.utils.Utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;


public class WordCountTopology {
	
	
	
  public static void main(String[] args)
      throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
    Properties properties = getResourceProperties();

    String kafkaBootstrapServer = properties.getProperty("kafka.bootstrap.server");
    String topicNameInput = properties.getProperty("kafka.topic.input");
    String topicNameOutput = properties.getProperty("kafka.topic.output");

    KafkaSpoutConfig<String,String> kafkaSpoutConfig = KafkaSpoutConfig.builder(kafkaBootstrapServer,topicNameInput).build();
    KafkaSpout<String,String> kafkaSpoutInput = new KafkaSpout<>(kafkaSpoutConfig);

    //Start building topology
    TopologyBuilder builder = new TopologyBuilder();

    //This will add spout to topology i.e. Kafka reader
    builder.setSpout("kafka-spout", kafkaSpoutInput, 1);

    //This will add bolt to topology which read output from spout
    builder.setBolt("sentence-splitter", new SentenceSplitterBolt(), 1).
        shuffleGrouping("kafka-spout");


    KafkaBolt outputBolt = new KafkaBolt().withProducerProperties(readKafkaProperties(properties))
        .withTopicSelector(new DefaultTopicSelector(topicNameOutput))
        .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());

    builder.setBolt("output-bolt",outputBolt,1).shuffleGrouping("sentence-splitter");

    Config config = new Config();
    config.setDebug(true);
    config.setMessageTimeoutSecs(30);
	Utils.readDefaultConfig();
	config.putAll(Utils.readDefaultConfig());
	
	config.put(Config.STORM_CLUSTER_MODE, "local");
	int port = 6627;
	config.put(Config.NIMBUS_THRIFT_PORT, port);
    config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1000);
    config.put(Config.TOPOLOGY_ACKER_EXECUTORS,1);

    /*
    //To run this code locally.... uncomment these lines and comment the next line
    LocalCluster localCluster = new LocalCluster();
    localCluster.submitTopology("WordCountTopology",new Config(),builder.createTopology());
    */
	HashMap<String,Object> localClusterConf = new HashMap();
	localClusterConf.put("storm.cluster.mode", "local");
	//localClusterConf.put("nimbus-daemon", true);
	localClusterConf.put("nimbus.thrift.port", port);
	//localClusterConf.put("nimbus.seeds" , nimbus_seeds);
	//--localClusterConf.put("nimbus.topology.validator" ,StrictTopologyValidator.class.getName());
	ILocalCluster cluster = Testing.getLocalCluster(localClusterConf);
	//cluster.activate("Random-integer-Topology");
	try
	{
		cluster.submitTopology("WordCountTopolgy",config,builder.createTopology());
		Thread.sleep(30000);
	}
	catch (InterruptedException e)
	{
		e.printStackTrace();
	} catch (AlreadyAliveException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InvalidTopologyException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }

  private static Properties readKafkaProperties(Properties configurationProps) {
    Properties props = new Properties();

    props.setProperty("bootstrap.servers",configurationProps.getProperty("kafka.bootstrap.server"));
    props.setProperty("acks",configurationProps.getProperty("acks"));
    props.setProperty("key.serializer",configurationProps.getProperty("key.serializer"));
    props.setProperty("value.serializer",configurationProps.getProperty("value.serializer"));
    props.setProperty("auto.offset.reset",configurationProps.getProperty("kafka.offset.reset","smallest"));

    return props;
  }

  private static Properties getResourceProperties() {
    String resourceName = "application.properties";
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    Properties props = new Properties();
    try (InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
      props.load(resourceStream);
    } catch (IOException e1) {
      e1.printStackTrace();
    }
    return props;
  }
}
