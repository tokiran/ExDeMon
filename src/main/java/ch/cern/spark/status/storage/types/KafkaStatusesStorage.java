package ch.cern.spark.status.storage.types;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.atomic.LongAccumulator;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import ch.cern.components.RegisterComponent;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.JSONStatusSerializer;
import ch.cern.spark.status.storage.JavaStatusSerializer;
import ch.cern.spark.status.storage.StatusSerializer;
import ch.cern.spark.status.storage.StatusesStorage;
import ch.cern.utils.ByteArray;
import scala.Tuple2;

@RegisterComponent("kafka")
public class KafkaStatusesStorage extends StatusesStorage {

	private static final long serialVersionUID = 1194347587683707148L;
	
	private transient final static Logger LOG = Logger.getLogger(KafkaStatusesStorage.class.getName());

    public static final int MAX_RECORD_SIZE = 1048576;
	
	private Map<String, Object> kafkaProducerParams = null;
	private Map<String, Object> kafkaConsumerParams;

	private String topic;
	
	private StatusSerializer serializer;

	private transient KafkaConsumer<Bytes, Bytes> consumer;

    private Duration timeout;
	
	public void config(Properties properties) throws ConfigurationException {
		kafkaProducerParams = getKafkaProducerParams(properties);
		kafkaConsumerParams = getKafkaConsumerParams(properties);
        
		topic = properties.getProperty("topic");
		
		timeout = properties.getPeriod("timeout", Duration.ofSeconds(2));
		
		String serializationType = properties.getProperty("serialization", "json");
		switch (serializationType) {
		case "json":
			serializer = new JSONStatusSerializer();
			break;
		case "java":
			serializer = new JavaStatusSerializer();
			break;
		default:
			throw new ConfigurationException("Serialization type " + serializationType + " is not available.");
		}
        
		properties.confirmAllPropertiesUsed();
	}
	
	@Override
	public JavaRDD<Tuple2<StatusKey, StatusValue>> load(JavaSparkContext context) throws IOException, ConfigurationException {

	    JavaRDD<ConsumerRecordSer> kafkaContent = context.parallelize(getAllRecords());
	    
		JavaRDD<Tuple2<ByteArray, ByteArray>> latestRecords = getLatestRecords(kafkaContent);
		
		JavaRDD<Tuple2<StatusKey, StatusValue>> parsed = parseRecords(latestRecords);
		
		LOG.info("Statuses loaded from Kafka topic " + topic);
		
		parsed = parsed.persist(StorageLevel.MEMORY_AND_DISK());
		
        return parsed;
	}

    private void setUpConsumer() {
        kafkaConsumerParams.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        
        consumer = new KafkaConsumer<Bytes, Bytes>(kafkaConsumerParams);
        consumer.subscribe(Sets.newHashSet(topic));
    }

    private List<ConsumerRecordSer> getAllRecords() {
        setUpConsumer();
        
        Map<Bytes, ConsumerRecordSer> latestRecords = new HashMap<>();
        
        int num_partitions = getNumberOfPartitions();
        
        long[] lastOffsets = new long[num_partitions];
        
        LongAccumulator[] processed_records_count = new LongAccumulator[num_partitions];
        for (int i = 0; i < processed_records_count.length; i++)
            processed_records_count[i] = new LongAccumulator((x,  y) -> x + y, 0L);

        ConsumerRecords<Bytes, Bytes> records = consumer.poll(timeout.toMillis());
        while(!records.isEmpty()) {
            records.forEach(r -> {
                long offset = r.offset();
                if(lastOffsets[r.partition()] < offset)
                    lastOffsets[r.partition()] = offset;
                
                processed_records_count[r.partition()].accumulate(1L);
                
                if(latestRecords.containsKey(r.key())) {
                    if(latestRecords.get(r.key()).offset() < r.offset()) {
                        latestRecords.put(r.key(), new ConsumerRecordSer(r));
                    }
                }else{
                    latestRecords.put(r.key(), new ConsumerRecordSer(r));
                }
            });

            records = consumer.poll(timeout.toMillis());
        }
        
        checkAllRecordsConsumed(lastOffsets);
        
        consumer.close();
        
        LOG.info(Arrays.stream(processed_records_count).mapToDouble(a -> a.longValue()).sum() + " records processed, " + latestRecords.size() + " uniques keys");
        LOG.info(Arrays.toString(Arrays.stream(processed_records_count).mapToDouble(a -> a.longValue()).toArray()) + " (records per partition)");
        
        return new LinkedList<>(latestRecords.values());
    }
    
    private int getNumberOfPartitions() {
        return consumer.partitionsFor(topic).size();
    }

    public void checkAllRecordsConsumed(long[] lastOffsets) {
        List<TopicPartition> partitions = new LinkedList<>();
        for (PartitionInfo partitionInfo : consumer.partitionsFor(topic))
            partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));

        long[] until = new long[partitions.size()];

        consumer.seekToEnd(partitions);
        consumer.poll(0);
        for (TopicPartition tp : partitions) {
            until[tp.partition()] = consumer.position(tp) - 1;
            
            if(until[tp.partition()] < 0)
                until[tp.partition()] = 0;
        }
        
        if(!Arrays.equals(until, lastOffsets)) {
            LOG.error("Some partitions were not completelly consumed when reading the state.");
            LOG.error("Topic " + topic + " last partition offsets: " + Arrays.toString(until));
            LOG.error("Consumed from topic " + topic + " till offsets: " + Arrays.toString(lastOffsets));
            
            throw new RuntimeException("Topic has not been completelly consumed.");
        }
    }

    private JavaRDD<Tuple2<ByteArray, ByteArray>> getLatestRecords(JavaRDD<ConsumerRecordSer> kafkaContent) {
		return kafkaContent.mapToPair(consumedRecord -> {
									Tuple2<Long, ByteArray> value = new Tuple2<>(consumedRecord.offset(), consumedRecord.value());
									
									return new Tuple2<ByteArray, Tuple2<Long, ByteArray>>(consumedRecord.key(), value);
								})
								.groupByKey()
								.map(pair -> getLastRecord(pair))
								.filter(pair -> pair._2 != null);
	}
	
    @VisibleForTesting
	protected static Tuple2<ByteArray, ByteArray> getLastRecord(Tuple2<ByteArray, Iterable<Tuple2<Long, ByteArray>>> pair) {
        ByteArray key = pair._1;
        
        Iterator<Tuple2<Long, ByteArray>> values = pair._2.iterator();
        
        Tuple2<Long, ByteArray> latestValue = values.next();
        
        while(values.hasNext()) {
            Tuple2<Long, ByteArray> value = values.next();
            
            if(value._1 > latestValue._1)
                latestValue = value;
        }
        
        return new Tuple2<ByteArray, ByteArray>(key, latestValue._2);
    }

    private JavaRDD<Tuple2<StatusKey, StatusValue>> parseRecords(JavaRDD<Tuple2<ByteArray, ByteArray>> latestRecords) {
		return latestRecords.map(binaryRecord -> new Tuple2<>(
														serializer.toKey(binaryRecord._1.get()),
														serializer.toValue(binaryRecord._2.get()))
													);
	}
	
	@Override
	public <K extends StatusKey, V extends StatusValue> void save(JavaPairRDD<K, V> rdd, Time time)
			throws IllegalArgumentException, IOException, ConfigurationException {
		
		rdd = rdd.filter(tuple -> isUpdatedState(tuple, time));
		
		rdd.foreachPartitionAsync(new KafkaProducerFunc<K, V>(kafkaProducerParams, serializer, topic));
	}
	
    private <K extends StatusKey, V extends StatusValue> boolean isUpdatedState(Tuple2<K, V> tuple, Time time) {
        return tuple._2 == null 
                || tuple._2.getStatus_update_time() == time.milliseconds() 
                || tuple._2.getStatus_update_time() == 0l;
    }

    @Override
    public <K extends StatusKey> void remove(JavaRDD<K> rdd) {
        JavaRDD<Tuple2<K, StatusValue>> keyWithNulls = rdd.map(key -> new Tuple2<K, StatusValue>(key, null));
        
        keyWithNulls.foreachPartitionAsync(new KafkaProducerFunc<K, StatusValue>(kafkaProducerParams, serializer, topic));
    }

	private Map<String, Object> getKafkaProducerParams(Properties props) {
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        
        kafkaParams.put("key.serializer", BytesSerializer.class);
        kafkaParams.put("value.serializer", BytesSerializer.class);
        
        Properties kafkaPropertiesFromConf = props.getSubset("producer");
        for (Entry<Object, Object> kafkaPropertyFromConf : kafkaPropertiesFromConf.entrySet()) {
            String key = (String) kafkaPropertyFromConf.getKey();
            String value = (String) kafkaPropertyFromConf.getValue();
            
            kafkaParams.put(key, value);
        }
        
        return kafkaParams;
    }
	
    private Map<String, Object> getKafkaConsumerParams(Properties props) {
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        
        kafkaParams.put("key.deserializer", BytesDeserializer.class);
        kafkaParams.put("value.deserializer", BytesDeserializer.class);
       
        kafkaParams.put(ConsumerConfig.CLIENT_ID_CONFIG, "spark-metrics-monitor");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        Properties kafkaPropertiesFromConf = props.getSubset("consumer");
        for (Entry<Object, Object> kafkaPropertyFromConf : kafkaPropertiesFromConf.entrySet()) {
            String key = (String) kafkaPropertyFromConf.getKey();
            String value = (String) kafkaPropertyFromConf.getValue();
            
            kafkaParams.put(key, value);
        }
        
        return kafkaParams;
    }
    
    private static class KafkaProducerFunc<K extends StatusKey, V extends StatusValue> implements VoidFunction<Iterator<Tuple2<K, V>>>{

		private static final long serialVersionUID = 3712180876662835316L;
		
		private Map<String, Object> props;

		private String topic;

		private StatusSerializer serializer;
		
		protected KafkaProducerFunc(Map<String, Object> props, StatusSerializer serializer, String topic) {
			this.props = props;
			this.topic = topic;
			this.serializer = serializer;
		}

		@Override
		public void call(Iterator<Tuple2<K, V>> records) throws Exception {
			KafkaProducer<Bytes, Bytes> producer = new KafkaProducer<>(props);
			
			while(records.hasNext()) {
				Tuple2<K, V> tuple = records.next();

				Bytes key = tuple._1 != null ? new Bytes(serializer.fromKey(tuple._1)) : null;
				Bytes value = tuple._2 != null ? new Bytes(serializer.fromValue(tuple._2)) : null;
				
				ProducerRecord<Bytes, Bytes> record = new ProducerRecord<Bytes, Bytes>(topic, key, value);
				
				int record_size = 0;
				if(key != null)
				    record_size += key.get().length;
				if(value != null)
                    record_size += value.get().length;
				
				if(record_size > MAX_RECORD_SIZE)
				    LOG.warn("Record is too large (" + record_size + " bytes). Key=" + tuple._1);
				    
				producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(exception != null)
                            LOG.error("Kafka exception when sending record", exception);
                    }
                });
			}
			
			producer.flush();
			producer.close();
		}
    	
    }

}
