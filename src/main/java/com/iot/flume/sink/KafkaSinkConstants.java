package com.iot.flume.sink;

/**
 * Created by hjl on 2016/8/26.
 */
public class KafkaSinkConstants {

    public static final String PROPERTY_PREFIX = "kafka.";

  /* Properties */

    public static final String TOPIC = "topic";
    public static final String BATCH_SIZE = "batch.size";
    public static final String MESSAGE_SERIALIZER_KEY = "value.serializer.class";
    public static final String KEY_SERIALIZER_KEY = "key.serializer.class";
    public static final String BROKER_LIST_KEY = "bootstrap.servers";
    public static final String REQUIRED_ACKS_KEY = "acks";
    public static final String BROKER_LIST_FLUME_KEY = "bootstrap.servers";
    public static final String REQUIRED_ACKS_FLUME_KEY = "acks";
    public static final String PREPROCESSOR="preprocessor";


    public static final int DEFAULT_BATCH_SIZE = 100;
    public static final String DEFAULT_TOPIC = "default-flume-topic";
    public static final String DEFAULT_VALUE_SERIALIZER =
            "org.apache.kafka.common.serialization.StringSerializer";
    public static final String DEFAULT_KEY_SERIALIZER =
            "org.apache.kafka.common.serialization.StringSerializer";
    public static final String DEFAULT_REQUIRED_ACKS = "1";

}
