package com.iot.flume.sink;

import com.google.common.base.Throwables;
import com.iot.flume.processor.MessagePreprocessor;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;


/**
 * Created by hjl on 2016/8/26.
 */
public class NewProducerKafkaSink extends AbstractSink implements Configurable {


    private static final Logger logger = LoggerFactory.getLogger(NewProducerKafkaSink.class);
    private Properties producerProps;
    public static final String KEY_HDR = "key";
    public static final String TOPIC_HDR = "topic";
    public static final String PARTITION_HDR = "partition";
    private KafkaProducer<String, String> producer;
    private MessagePreprocessor messagePreProcessor;
    private Context context;
    private String topic;

    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event = null;

        try {
            transaction.begin();
            event = channel.take();
            if (event != null) {
                // get the message body.
                Map<String, String> headers = event.getHeaders();
                String eventTopic;
                String eventBody;
                int partition;
                String eventKey;
                partition = Integer.valueOf(headers.get(PARTITION_HDR));
                // if the metadata extractor is set, extract the topic and the key.
                if (messagePreProcessor != null) {
                    eventTopic = messagePreProcessor.extractTopic(event, context);
                    eventBody = messagePreProcessor.transformMessage(event, context);
                    eventKey = messagePreProcessor.extractKey(event, context);
                } else {

                    if (null == (eventTopic = headers.get(TOPIC_HDR))) {
                        eventTopic = topic;
                    }
                    eventBody = new String(event.getBody(), "UTF-8");
                    eventKey = headers.get(KEY_HDR);

                }

                // log the event for debugging
                if (logger.isDebugEnabled()) {
                    logger.debug("{Event} " + eventBody);
                }
                // create a message
                ProducerRecord<String, String> data = new ProducerRecord<String, String>(eventTopic, partition, eventKey, eventBody);
                // publish
                producer.send(data);
            } else {
                // No event found, request back-off semantics from the sink runner
                result = Status.BACKOFF;
                break;
            }
            // publishing is successful. Commit.
            transaction.commit();

        } catch (Exception ex) {
            String errorMsg = "Failed to publish events";
            logger.error("Failed to publish events", ex);
            result = Status.BACKOFF;
            if (transaction != null) {
                try {
                    transaction.rollback();
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
        producer = new KafkaProducer<String, String>(producerProps);
        super.start();
    }

    @Override
    public synchronized void stop() {
        producer.close();
        super.stop();
    }


    public void configure(Context context) {
        this.context = context;

        producerProps = NewProducerKafkaSinkUtil.getKafkaProperties(context);

        // get the message Preprocessor if set
        String preprocessorClassName = context.getString(NewProducerKafkaSinkConstants.PREPROCESSOR);
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

        if (messagePreProcessor == null) {
            // MessagePreprocessor2 is not set. So read the topic from the config.
            topic = context.getString(NewProducerKafkaSinkConstants.TOPIC, NewProducerKafkaSinkConstants.DEFAULT_TOPIC);
            if (topic.equals(NewProducerKafkaSinkConstants.DEFAULT_TOPIC)) {
                logger.warn("The Properties 'metadata.extractor' or 'topic' is not set. Using the default topic name" +
                        NewProducerKafkaSinkConstants.DEFAULT_TOPIC);
            } else {
                logger.info("Using the static topic: " + topic);
            }
        }
    }
}
