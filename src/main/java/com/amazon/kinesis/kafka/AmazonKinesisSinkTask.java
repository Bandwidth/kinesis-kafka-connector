package com.amazon.kinesis.kafka;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.Shard;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.util.StringUtils;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class AmazonKinesisSinkTask extends SinkTask {

	private static final Logger logger = LoggerFactory.getLogger(AmazonKinesisSinkTask.class);

	private String streamName;

	private String regionName;

	private String roleARN;

	private String roleExternalID;

	private String roleSessionName;

	private int roleDurationSeconds;

	private String kinesisEndpoint;

	private int maxConnections;

	private int rateLimit;

	private int maxBufferedTime;

	private int ttl;

	private String metricsLevel;

	private String metricsGranuality;

	private String metricsNameSpace;

	private boolean aggregation;

	private boolean usePartitionAsHashKey;

	private int totalKafkaPartitions;

	private boolean flushSync;

	private boolean singleKinesisProducerPerPartition;

	private boolean pauseConsumption;

	private int outstandingRecordsThreshold;

	private int sleepPeriod;

	private int sleepCycles;

	private SinkTaskContext sinkTaskContext;

	private Map<String, KinesisProducer> producerMap = new HashMap<String, KinesisProducer>();

	private KinesisProducer kinesisProducer;

	private ConnectException putException;

	private static final BigInteger MAX_HASH = BigInteger.valueOf(2).pow(128);

	private List<Shard> kinesisShardsCache = null;
	private final Object kinesisShardsCacheLock = new Object();
	private ScheduledExecutorService shardCacheRefresher;
	private static final long SHARD_CACHE_REFRESH_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

	private final Random rand = new Random();
	private final MessageDigest md5 = getMD5();

	private static MessageDigest getMD5() {
		try {
			return MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("Failed to get MD5 algorithm", e);
		}
	}

	final FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>() {
		@Override
		public void onFailure(Throwable t) {
			if (t instanceof UserRecordFailedException) {
				Attempt last = Iterables.getLast(((UserRecordFailedException) t).getResult().getAttempts());
				putException =  new RetriableException("Kinesis Producer was not able to publish data - " + last.getErrorCode() + "-"
						+ last.getErrorMessage());
				return;
			}
			putException = new RetriableException("Exception during Kinesis put", t);
		}

		@Override
		public void onSuccess(UserRecordResult result) {

		}
	};

	@Override
	public void initialize(SinkTaskContext context) {
		sinkTaskContext = context;

		// Start scheduled cache refresh
		shardCacheRefresher = Executors.newSingleThreadScheduledExecutor();
		shardCacheRefresher.scheduleAtFixedRate(() -> {
			try {
				List<Shard> shards = getKinesisShards();
				synchronized (kinesisShardsCacheLock) {
					kinesisShardsCache = shards;
				}
				logger.info("Kinesis shard cache refreshed, {} shards loaded.", shards.size());
			} catch (Exception e) {
				logger.error("Error refreshing Kinesis shard cache", e);
			}
		}, 0, SHARD_CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
	}

	@Override
	public String version() {
		return null;
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> arg0) {
		checkForEarlierPutException();

		if (singleKinesisProducerPerPartition) {
			producerMap.values().forEach(producer -> {
				if (flushSync)
					producer.flushSync();
				else
					producer.flush();
			});
		} else {
			if (flushSync)
				kinesisProducer.flushSync();
			else
				kinesisProducer.flush();
		}
	}

	@Override
	public void put(Collection<SinkRecord> sinkRecords) {
		checkForEarlierPutException();

		// If KinesisProducers cannot write to Kinesis Streams (because of
		// connectivity issues, access issues
		// or misconfigured shards we will pause consumption of messages till
		// backlog is cleared

		validateOutStandingRecords();

		String partitionKey;
		for (SinkRecord sinkRecord : sinkRecords) {

			ListenableFuture<UserRecordResult> f;
			// Kinesis does not allow empty partition key
			if (sinkRecord.key() != null && !sinkRecord.key().toString().trim().equals("")) {
				partitionKey = sinkRecord.key().toString().trim();
			} else {
				partitionKey = Integer.toString(sinkRecord.kafkaPartition());
			}

			if (singleKinesisProducerPerPartition)
				f = addUserRecord(producerMap.get(sinkRecord.kafkaPartition() + "@" + sinkRecord.topic()), streamName,
						partitionKey, usePartitionAsHashKey, sinkRecord);
			else
				f = addUserRecord(kinesisProducer, streamName, partitionKey, usePartitionAsHashKey, sinkRecord);

			Futures.addCallback(f, callback, MoreExecutors.directExecutor());

		}
	}

	private boolean validateOutStandingRecords() {
		if (pauseConsumption) {
			if (singleKinesisProducerPerPartition) {
				producerMap.values().forEach(producer -> {
					int sleepCount = 0;
					boolean pause = false;
					// Validate if producer has outstanding records within
					// threshold values
					// and if not pause further consumption
					while (producer.getOutstandingRecordsCount() > outstandingRecordsThreshold) {
						try {
							// Pausing further
							sinkTaskContext.pause((TopicPartition[]) sinkTaskContext.assignment().toArray(new TopicPartition[sinkTaskContext.assignment().size()]));
							pause = true;
							Thread.sleep(sleepPeriod);
							if (sleepCount++ > sleepCycles) {
								// Dummy message - Replace with your code to
								// notify/log that Kinesis Producers have
								// buffered values
								// but are not being sent
								logger.info(
										"Kafka Consumption has been stopped because Kinesis Producers has buffered messages above threshold");
								sleepCount = 0;
							}
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					if (pause)
						sinkTaskContext.resume((TopicPartition[]) sinkTaskContext.assignment().toArray(new TopicPartition[sinkTaskContext.assignment().size()]));
				});
				return true;
			} else {
				int sleepCount = 0;
				boolean pause = false;
				// Validate if producer has outstanding records within threshold
				// values
				// and if not pause further consumption
				while (kinesisProducer.getOutstandingRecordsCount() > outstandingRecordsThreshold) {
					try {
						// Pausing further
						sinkTaskContext.pause((TopicPartition[]) sinkTaskContext.assignment().toArray(new TopicPartition[sinkTaskContext.assignment().size()]));
						pause = true;
						Thread.sleep(sleepPeriod);
						if (sleepCount++ > sleepCycles) {
							// Dummy message - Replace with your code to
							// notify/log that Kinesis Producers have buffered
							// values
							// but are not being sent
							logger.info(
									"Kafka Consumption has been stopped because Kinesis Producers has buffered messages above threshold");
							sleepCount = 0;
						}
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				if (pause)
					sinkTaskContext.resume((TopicPartition[]) sinkTaskContext.assignment().toArray(new TopicPartition[sinkTaskContext.assignment().size()]));
				return true;
			}
		} else {
			return true;
		}
	}

	/**
	 * Examine whether an exception was reported from an earlier call to <code>put</code>.
	 * If so, then clear the exception and surface it up to Kafka Connect.
	 */
	private void checkForEarlierPutException() {
		if (putException != null) {
			final ConnectException e = putException;
			putException = null;
			throw e;
		}
	}

	private List<Shard> getCachedKinesisShards() {
		if (kinesisShardsCache != null) {
			return kinesisShardsCache;
		}

		synchronized (kinesisShardsCacheLock) {
			kinesisShardsCache = getKinesisShards();
		}
		return kinesisShardsCache;
	}


	private ListenableFuture<UserRecordResult> addUserRecord(KinesisProducer kp, String streamName, String partitionKey,
			boolean usePartitionAsHashKey, SinkRecord sinkRecord) {

		// The schema wasn't getting set when running locally with Localstack.
		Schema valueSchema = sinkRecord.valueSchema();
		if (valueSchema == null) {
			logger.warn("Sink Record Schema is null for record: {}:{}:{}:{}. This only should happen locally.", sinkRecord.topic(), sinkRecord.kafkaPartition(),
					sinkRecord.key(), sinkRecord.value());
			valueSchema = Schema.STRING_SCHEMA;
		}

		logger.debug("Adding user record for stream: {}, partitionKey: {}, kafkaPartition: {}",
				streamName, partitionKey, sinkRecord.kafkaPartition());

		if (usePartitionAsHashKey)
			return kp.addUserRecord(streamName, partitionKey, Integer.toString(sinkRecord.kafkaPartition()),
				DataUtility.parseValue(valueSchema, sinkRecord.value()));
		else
			return kp.addUserRecord(streamName, partitionKey,
				DataUtility.parseValue(valueSchema, sinkRecord.value()));
	}

	/**
	 * Handles spreading the partition key across multiple shards if there are more shards than
	 * Kafka partitions. This helps to avoid hot-sharding when there are few Kafka partitions.
	 * If there are fewer shards than Kafka partitions, only one shard will be used per Kafka partition.
	 */
	private int selectShardWithSpread(String partitionKey, int numShards, int numKafkaPartitions) {
		// Determine how many shards we should spread across.
		int numShardsToSpreadAcross = (int) Math.ceil((double) numShards / numKafkaPartitions);

		// Pick a random integer between 0 and numShardsToSpreadAcross - 1
		int spreadShard = rand.nextInt(numShardsToSpreadAcross);

		String spreadPartitionKey = partitionKey + "-" + spreadShard;

		logger.debug("Spreading partition key {} across {} shards, using spread partition key {}. (Total shards: {} and kafka partitions: {})",
				partitionKey, numShardsToSpreadAcross, spreadPartitionKey, numShards, numKafkaPartitions);

		return selectShard(spreadPartitionKey, numShards);
	}

	/**
	 * Selects a shard index based on the provided partition key and number of shards.
	 * It uses MD5 hashing to ensure an even distribution across shards.
	 */
	private int selectShard(String partitionKey, int numShards) {
		byte[] digest = md5.digest(partitionKey.getBytes(StandardCharsets.UTF_8));

		// Create a hash of the partition key
		BigInteger hashedPartitionKey = new BigInteger(1, digest);

		// Calculate how much of the hash space each shard covers
		BigInteger hashSpacePerShard = MAX_HASH.divide(java.math.BigInteger.valueOf(numShards));

		// Find where in our hash space the hashed partition key lands
		BigInteger shardId = hashedPartitionKey.divide(hashSpacePerShard);
		int shardIndex = shardId.intValue();
		if (shardIndex < 0 || shardIndex >= numShards) {
			throw new IllegalArgumentException("Shard index out of bounds: " + shardIndex + " for numShards: " + numShards);
		}

		return shardIndex;
	}

	private List<Shard> getKinesisShards() {
		AmazonKinesis kinesisClient = AmazonKinesisClientBuilder.standard()
				.withRegion(regionName)
				.build();
		List<Shard> shards = new ArrayList<>();
		String exclusiveStartShardId = null;
		do {
			DescribeStreamRequest request = new DescribeStreamRequest()
					.withStreamName(streamName);
			if (exclusiveStartShardId != null) {
				request.setExclusiveStartShardId(exclusiveStartShardId);
			}
			DescribeStreamResult result = kinesisClient.describeStream(request);
			shards.addAll(result.getStreamDescription().getShards());
			if (result.getStreamDescription().getHasMoreShards()) {
				exclusiveStartShardId = result.getStreamDescription().getShards()
						.get(result.getStreamDescription().getShards().size() - 1)
						.getShardId();
			} else {
				exclusiveStartShardId = null;
			}

		} while (exclusiveStartShardId != null);
		kinesisClient.shutdown();
		// sorted shards by shardId to ensure consistent ordering
		List<Shard> sortedShards = new ArrayList<>(shards);
		sortedShards.sort(Comparator.comparing(Shard::getShardId));

		return sortedShards;
	}

	@Override
	public void start(Map<String, String> props) {

		streamName = props.get(AmazonKinesisSinkConnector.STREAM_NAME);

		maxConnections = Integer.parseInt(props.get(AmazonKinesisSinkConnector.MAX_CONNECTIONS));

		rateLimit = Integer.parseInt(props.get(AmazonKinesisSinkConnector.RATE_LIMIT));

		maxBufferedTime = Integer.parseInt(props.get(AmazonKinesisSinkConnector.MAX_BUFFERED_TIME));

		ttl = Integer.parseInt(props.get(AmazonKinesisSinkConnector.RECORD_TTL));

		regionName = props.get(AmazonKinesisSinkConnector.REGION);

		roleARN = props.get(AmazonKinesisSinkConnector.ROLE_ARN);

		roleSessionName = props.get(AmazonKinesisSinkConnector.ROLE_SESSION_NAME);

		roleDurationSeconds =  Integer.parseInt(props.get(AmazonKinesisSinkConnector.ROLE_DURATION_SECONDS));

		roleExternalID = props.get(AmazonKinesisSinkConnector.ROLE_EXTERNAL_ID);

		kinesisEndpoint = props.get(AmazonKinesisSinkConnector.KINESIS_ENDPOINT);

		metricsLevel = props.get(AmazonKinesisSinkConnector.METRICS_LEVEL);

		metricsGranuality = props.get(AmazonKinesisSinkConnector.METRICS_GRANUALITY);

		metricsNameSpace = props.get(AmazonKinesisSinkConnector.METRICS_NAMESPACE);

		aggregation = Boolean.parseBoolean(props.get(AmazonKinesisSinkConnector.AGGREGATION_ENABLED));

		usePartitionAsHashKey = Boolean.parseBoolean(props.get(AmazonKinesisSinkConnector.USE_PARTITION_AS_HASH_KEY));

		totalKafkaPartitions = Integer.parseInt(props.get(AmazonKinesisSinkConnector.TOTAL_KAFKA_PARTITIONS));

		flushSync = Boolean.parseBoolean(props.get(AmazonKinesisSinkConnector.FLUSH_SYNC));

		singleKinesisProducerPerPartition = Boolean
				.parseBoolean(props.get(AmazonKinesisSinkConnector.SINGLE_KINESIS_PRODUCER_PER_PARTITION));

		pauseConsumption = Boolean.parseBoolean(props.get(AmazonKinesisSinkConnector.PAUSE_CONSUMPTION));

		outstandingRecordsThreshold = Integer
				.parseInt(props.get(AmazonKinesisSinkConnector.OUTSTANDING_RECORDS_THRESHOLD));

		sleepPeriod = Integer.parseInt(props.get(AmazonKinesisSinkConnector.SLEEP_PERIOD));

		sleepCycles = Integer.parseInt(props.get(AmazonKinesisSinkConnector.SLEEP_CYCLES));

		if (!singleKinesisProducerPerPartition)
			kinesisProducer = getKinesisProducer();

		putException = null;

	}

	public void open(Collection<TopicPartition> partitions) {
		if (singleKinesisProducerPerPartition) {
			for (TopicPartition topicPartition : partitions) {
				producerMap.put(topicPartition.partition() + "@" + topicPartition.topic(), getKinesisProducer());
			}
		}
	}

	public void close(Collection<TopicPartition> partitions) {
		if (singleKinesisProducerPerPartition) {
			for (TopicPartition topicPartition : partitions) {
				producerMap.get(topicPartition.partition() + "@" + topicPartition.topic()).destroy();
				producerMap.remove(topicPartition.partition() + "@" + topicPartition.topic());
			}
		}
	}

	@Override
	public void stop() {
		if (shardCacheRefresher != null) {
			shardCacheRefresher.shutdownNow();
		}
		// destroying kinesis producers which were not closed as part of close
		if (singleKinesisProducerPerPartition) {
			for (KinesisProducer kp : producerMap.values()) {
				kp.flushSync();
				kp.destroy();
			}
		} else {
			kinesisProducer.destroy();
		}

	}

	private KinesisProducer getKinesisProducer() {
		KinesisProducerConfiguration config = new KinesisProducerConfiguration();
		config.setRegion(regionName);
		config.setCredentialsProvider(IAMUtility.createCredentials(regionName, roleARN, roleExternalID, roleSessionName, roleDurationSeconds));
		config.setMaxConnections(maxConnections);
		if (!StringUtils.isNullOrEmpty(kinesisEndpoint)) {
			try {
				URL kinesisEndpointUrl = new URL(kinesisEndpoint);
				config.setKinesisEndpoint(kinesisEndpointUrl.getHost());
				config.setKinesisPort(kinesisEndpointUrl.getPort());

				// If we are using localstack, we need to disable certificate validation
				// as localstack uses self-signed certificates
				if ("https".equals(kinesisEndpointUrl.getProtocol()) && "localstack".equals(
						kinesisEndpointUrl.getHost())) {
					config.setVerifyCertificate(false);
				}
			} catch (MalformedURLException e) {
				throw new RuntimeException(e);
			}
		}

		config.setAggregationEnabled(aggregation);

		// Limits the maximum allowed put rate for a shard, as a percentage of
		// the
		// backend limits.
		config.setRateLimit(rateLimit);

		// Maximum amount of time (milliseconds) a record may spend being
		// buffered
		// before it gets sent. Records may be sent sooner than this depending
		// on the
		// other buffering limits
		config.setRecordMaxBufferedTime(maxBufferedTime);

		// Set a time-to-live on records (milliseconds). Records that do not get
		// successfully put within the limit are failed.
		config.setRecordTtl(ttl);

		// Controls the number of metrics that are uploaded to CloudWatch.
		// Expected pattern: none|summary|detailed
		config.setMetricsLevel(metricsLevel);

		// Controls the granularity of metrics that are uploaded to CloudWatch.
		// Greater granularity produces more metrics.
		// Expected pattern: global|stream|shard
		config.setMetricsGranularity(metricsGranuality);

		// The namespace to upload metrics under.
		config.setMetricsNamespace(metricsNameSpace);

		return new KinesisProducer(config);

	}
}
