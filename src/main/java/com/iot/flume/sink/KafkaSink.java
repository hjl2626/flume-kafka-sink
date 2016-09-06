package com.iot.flume.sink;

import com.google.common.base.Throwables;
import com.iot.flume.processor.MessagePreprocessor;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.kafka.KafkaSinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Created by hjl on 2016/8/31.
 */
public class KafkaSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);
    private Properties kafkaProps;
    private KafkaSinkCounter counter;
    private List<Future<RecordMetadata>> kafkaFutures;
    private Integer batchSize;
    private static final String KEY_HDR = "key";
    private static final String TOPIC_HDR = "topic";
    private static final String PARTITION_HDR = "partition";
    private String topic;
    private String default_type;
    private KafkaProducer<String, String> producer;
    private MessagePreprocessor messagePreProcessor;
    private Context context;

    public Status process() throws EventDeliveryException {


        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
        Event event = null;
        String eventTopic = null;
        String eventKey = null;
        String eventBody = null;
        Integer eventPartition = null;

        try {
            long processedEvents = 0;

            transaction = channel.getTransaction();
            transaction.begin();

            kafkaFutures.clear();
            long batchStartTime = System.nanoTime();
            for (; processedEvents < batchSize; processedEvents += 1) {
                event = channel.take();

                if (event == null) {
                    // no events available in channel
                    if (processedEvents == 0) {
                        result = Status.BACKOFF;
                        counter.incrementBatchEmptyCount();
                    } else {
                        counter.incrementBatchUnderflowCount();
                    }
                    break;
                }

                Map<String, String> headers = event.getHeaders();
                eventPartition = Integer.valueOf(headers.get("partition"));
                if (messagePreProcessor != null) {
                    eventTopic = messagePreProcessor.extractTopic(event, context);
                    eventBody = messagePreProcessor.transformMessage(event, context);
                    eventKey = messagePreProcessor.extractKey(event, context);
                } else {
                    String type = headers.get("type");
                    String app = headers.get("app");
                    if (app == null && type == null) {
                        eventTopic = topic;
                    } else {
                        if (null == type) {
                            eventTopic = app + "." + default_type;
                        } else {
                            eventTopic = app + "." + type;
                        }
                    }
                    eventBody = new String(event.getBody(), "UTF-8");
                    eventKey = headers.get(KEY_HDR);
                    if (eventKey == null) {
                        eventKey = eventBody;
                    }
                }

                logger.info("\n\n eventBody=  " + eventBody + "\n" + "eventTopic = " + eventTopic + "\n" + "partition = " + eventPartition + "\n\n");
                // create a message and add to buffer
                long startTime = System.currentTimeMillis();
                if (eventPartition == null) {
                    kafkaFutures.add(producer.send(
                            new ProducerRecord<String, String>(eventTopic, eventKey, eventBody), new SinkCallback(startTime)));
                } else {
                    kafkaFutures.add(producer.send(
                            new ProducerRecord<String, String>(eventTopic, eventPartition, eventKey, eventBody), new SinkCallback(startTime)));
                }
            }

            if (processedEvents > 0) {
                for (Future<RecordMetadata> future : kafkaFutures) {
                    future.get();
                }
                long endTime = System.nanoTime();
                counter.addToKafkaEventSendTimer((endTime - batchStartTime) / (1000 * 1000));
                counter.addToEventDrainSuccessCount((long) kafkaFutures.size());
            }
            // publish batch and commit.
            transaction.commit();

        } catch (Exception ex) {
            String errorMsg = "Failed to publish events";
            logger.error("Failed to publish events", ex);
            result = Status.BACKOFF;
            if (transaction != null) {
                try {
                    kafkaFutures.clear();
                    transaction.rollback();
                    counter.incrementRollbackCount();
                } catch (Exception e) {
                    logger.error("Transaction rollback failed", e);
                    throw Throwables.propagate(e);
                }
            }
            throw new EventDeliveryException(errorMsg, ex);
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }

        return result;
    }


    @Override
    public synchronized void start() {
        // instantiate the producer
        producer = new KafkaProducer<String, String>(kafkaProps);
        counter.start();
        super.start();
    }

    @Override
    public synchronized void stop() {
        producer.close();
        counter.stop();
        logger.info("Kafka Sink {} stopped. Metrics: {}", getName(), counter);
        super.stop();
    }

    public void configure(Context context) {

        this.context = context;

        kafkaProps = KafkaSinkUtil.getKafkaProperties(context);

        kafkaFutures = new LinkedList<Future<RecordMetadata>>();

        batchSize = context.getInteger(KafkaSinkConstants.BATCH_SIZE_FLUME_KEY, KafkaSinkConstants.DEFAULT_BATCH_SIZE);

        default_type = context.getString(KafkaSinkConstants.TYPE_KEY, KafkaSinkConstants.DEFAULT_TYPE);

        topic = context.getString(KafkaSinkConstants.TOPIC, KafkaSinkConstants.DEFAULT_TOPIC);

        if (counter == null) {
            counter = new KafkaSinkCounter(getName());
        }
        // get the message Preprocessor if set
        String preprocessorClassName = context.getString(KafkaSinkConstants.PREPROCESSOR);
        // if it's set create an instance using Java Reflection.
        if (preprocessorClassName != null) {
            try {
                Class preprocessorClazz = Class.forName(preprocessorClassName.trim());
                Object preprocessorObj = preprocessorClazz.newInstance();
                if (preprocessorObj instanceof MessagePreprocessor) {
                    messagePreProcessor = (MessagePreprocessor) preprocessorObj;
                } else {
                    String errorMsg = "Provided class for MessagePreprocessor does not implement " +
                            "'com.iot.flume.processor.MessagePreprocessor'";
                    logger.error(errorMsg);
                    throw new IllegalArgumentException(errorMsg);
                }
            } catch (ClassNotFoundException e) {
                String errorMsg = "Error instantiating the MessagePreprocessor implementation.";
                logger.error(errorMsg, e);
                throw new IllegalArgumentException(errorMsg, e);
            } catch (InstantiationException e) {
                String errorMsg = "Error instantiating the MessagePreprocessor implementation.";
                logger.error(errorMsg, e);
                throw new IllegalArgumentException(errorMsg, e);
            } catch (IllegalAccessException e) {
                String errorMsg = "Error instantiating the MessagePreprocessor implementation.";
                logger.error(errorMsg, e);
                throw new IllegalArgumentException(errorMsg, e);
            }
        }

    }

}

class SinkCallback implements Callback {
    private static final Logger logger = LoggerFactory.getLogger(SinkCallback.class);
    private long startTime;

    public SinkCallback(long startTime) {
        this.startTime = startTime;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            logger.debug("Error sending message to Kafka {} ", exception.getMessage());
        }

        if (logger.isDebugEnabled()) {
            long eventElapsedTime = System.currentTimeMillis() - startTime;
            logger.debug("Acked message partition:{} offset:{}", metadata.partition(), metadata.offset());
            logger.debug("Elapsed time for send: {}", eventElapsedTime);
        }
    }

}
