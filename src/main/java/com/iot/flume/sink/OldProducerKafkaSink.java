package com.iot.flume.sink;

import com.google.common.base.Throwables;
import com.iot.flume.processor.MessagePreprocessor;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.kafka.KafkaSinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * Created by hjl on 2016/8/26.
 */
public class OldProducerKafkaSink extends AbstractSink implements Configurable {


    private static final Logger logger = LoggerFactory.getLogger(OldProducerKafkaSink.class);
    public static final String KEY_HDR = "key";
    public static final String TOPIC_HDR = "topic";
    public static final String PARTITION_HDR = "partition";
    private Properties kafkaProps;
    private Producer<String, String> producer;
    private String topic;
    private int batchSize;
    private List<KeyedMessage<String, String>> messageList;
    private KafkaSinkCounter counter;
    private MessagePreprocessor messagePreProcessor;
    private Context context;

    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
        Event event = null;
        String eventTopic = null;
        String eventKey = null;

        try {
            long processedEvents = 0;

            transaction = channel.getTransaction();
            transaction.begin();

            messageList.clear();
            for (; processedEvents < batchSize; processedEvents += 1) {
                event = channel.take();

                if (event == null) {
                    // no events available in channel
                    result = Status.BACKOFF;
                    break;
                }

                String eventBody = new String(event.getBody(),"UTF-8");
                Map<String, String> headers = event.getHeaders();

                if ((eventTopic = headers.get(TOPIC_HDR)) == null) {
                    eventTopic = topic;
                }

                eventKey = headers.get(PARTITION_HDR);

                if(messagePreProcessor != null){
                    eventBody = messagePreProcessor.transformMessage(event, context);
                    eventTopic = messagePreProcessor.extractTopic(event, context);
//                    eventKey = messagePreProcessor.extractKey(event, context);
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("{Event} " + eventTopic + " : " + eventKey + " : "
                            + eventBody);
                    logger.debug("event #{}", processedEvents);
                }
                // create a message and add to buffer
                KeyedMessage<String, String> data = new KeyedMessage<String,String>
                        (eventTopic, eventKey , eventKey, eventBody);
                messageList.add(data);
            }

            if (processedEvents == 0) {  
                counter.incrementBatchEmptyCount();  
                result = Status.BACKOFF;  
            } else {  
                if (processedEvents < batchSize) {  
                    counter.incrementBatchUnderflowCount();  
                } else {  
                    counter.incrementBatchCompleteCount();  
                }  

            // publish batch and commit.
            counter.addToEventDrainAttemptCount(processedEvents); 
            long startTime = System.nanoTime();  
            producer.send(messageList);
            long endTime = System.nanoTime();
            counter.addToKafkaEventSendTimer((endTime - startTime) / (1000 * 1000));
            counter.addToEventDrainSuccessCount(Long.valueOf(messageList.size()));
            
        }
            transaction.commit();

        } catch (Exception ex) {
            String errorMsg = "Failed to publish events";
            logger.error("Failed to publish events", ex);
            result = Status.BACKOFF;
            if (transaction != null) {
                try {
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
        ProducerConfig config = new ProducerConfig(kafkaProps);
        producer = new Producer<String, String>(config);
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


    /**
     * We configure the sink and generate properties for the Kafka Producer
     * <p>
     * Kafka producer properties is generated as follows:
     * 1. We generate a properties object with some static defaults that
     * can be overridden by Sink configuration
     * 2. We add the configuration users added for Kafka (parameters starting
     * with .kafka. and must be valid Kafka Producer properties
     * 3. We add the sink's documented parameters which can override other
     * properties
     *
     * @param context
     */
    public void configure(Context context) {
        this.context = context;

        batchSize = context.getInteger(OldProducerKafkaSinkConstants.BATCH_SIZE,
                OldProducerKafkaSinkConstants.DEFAULT_BATCH_SIZE);
        messageList =
                new ArrayList<KeyedMessage<String, String>>(batchSize);
        logger.debug("Using batch size: {}", batchSize);

        topic = context.getString(OldProducerKafkaSinkConstants.TOPIC,
                OldProducerKafkaSinkConstants.DEFAULT_TOPIC);
        if (topic.equals(OldProducerKafkaSinkConstants.DEFAULT_TOPIC)) {
            logger.warn("The Property 'topic' is not set. " +
                    "Using the default topic name: " +
                    OldProducerKafkaSinkConstants.DEFAULT_TOPIC);
        } else {
            logger.info("Using the static topic: " + topic +
                    " this may be over-ridden by event headers");
        }

        kafkaProps = OldProducerKafkaSinkUtil.getKafkaProperties(context);

        if (logger.isDebugEnabled()) {
            logger.debug("Kafka producer properties: " + kafkaProps);
        }

        if (counter == null) {
            counter = new KafkaSinkCounter(getName());
        }

        String preprocessorClassName = context.getString(OldProducerKafkaSinkConstants.PREPROCESSOR);
        // if it's set create an instance using Java Reflection.
        if (preprocessorClassName != null) {
            try {
                Class preprocessorClazz = Class.forName(preprocessorClassName.trim());
                Object preprocessorObj = preprocessorClazz.newInstance();
                if (preprocessorObj instanceof MessagePreprocessor) {
                    messagePreProcessor = (MessagePreprocessor) preprocessorObj;
                } else {
                    String errorMsg = "Provided class for MessagePreprocessor does not implement " +
                            "'com.thilinamb.flume.sink.MessagePreprocessor2'";
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


