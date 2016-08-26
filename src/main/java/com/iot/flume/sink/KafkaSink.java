package com.iot.flume.sink;

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
public class KafkaSink extends AbstractSink implements Configurable {



    private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);
    private Properties producerProps;
    private KafkaProducer<String, String> producer;
    private MessagePreprocessor messagePreProcessor;
    private Context context;
    private String topic;

    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event = null;
        String eventTopic = null;
        try {
            transaction.begin();
            event = channel.take();

            if (event != null) {
                // get the message body.
                String eventBody = new String(event.getBody());
                Integer partition = Integer.valueOf(event.getHeaders().get("partition"));
                eventTopic = context.getString("topic");
                // if the metadata extractor is set, extract the topic and the key.
                if (messagePreProcessor != null) {
                    eventBody = messagePreProcessor.transformMessage(event, context);
                }
                // log the event for debugging
                if (logger.isDebugEnabled()) {
                    logger.debug("{Event} " + eventBody);
                }
                // create a message
                ProducerRecord<String, String> data = new ProducerRecord<String, String>(eventTopic, partition,eventBody,eventBody);
                // publish
                producer.send(data);
            } else {
                // No event found, request back-off semantics from the sink runner
                result = Status.BACKOFF;
            }
            // publishing is successful. Commit.
            transaction.commit();

        } catch (Exception ex) {
            transaction.rollback();
            String errorMsg = "Failed to publish event: " + event;
            logger.error(errorMsg);
            throw new EventDeliveryException(errorMsg, ex);

        } finally {
            transaction.close();
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
                            "'com.thilinamb.flume.sink.MessagePreprocessor2'";
                    logger.error(errorMsg);
                    throw new IllegalArgumentException(errorMsg);
                }
            } catch (ClassNotFoundException e) {
                String errorMsg = "Error instantiating the MessagePreprocessor2 implementation.";
                logger.error(errorMsg, e);
                throw new IllegalArgumentException(errorMsg, e);
            } catch (InstantiationException e) {
                String errorMsg = "Error instantiating the MessagePreprocessor2 implementation.";
                logger.error(errorMsg, e);
                throw new IllegalArgumentException(errorMsg, e);
            } catch (IllegalAccessException e) {
                String errorMsg = "Error instantiating the MessagePreprocessor2 implementation.";
                logger.error(errorMsg, e);
                throw new IllegalArgumentException(errorMsg, e);
            }
        }

        if (messagePreProcessor == null) {
            // MessagePreprocessor2 is not set. So read the topic from the config.
            topic = context.getString(KafkaSinkConstants.TOPIC, KafkaSinkConstants.DEFAULT_TOPIC);
            if (topic.equals(KafkaSinkConstants.DEFAULT_TOPIC)) {
                logger.warn("The Properties 'metadata.extractor' or 'topic' is not set. Using the default topic name" +
                        KafkaSinkConstants.DEFAULT_TOPIC);
            } else {
                logger.info("Using the static topic: " + topic);
            }
        }
    }
}
