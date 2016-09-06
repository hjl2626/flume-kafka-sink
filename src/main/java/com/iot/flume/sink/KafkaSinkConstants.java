package com.iot.flume.sink;

/**
 * Created by hjl on 2016/8/31.
 */
public class KafkaSinkConstants {

    public static final String PROPERTY_PREFIX = "kafka.";

    /* Properties */

    public static final String TOPIC = "topic";

    public static final String VALUE_SERIALIZER_KEY = "value.serializer";
    public static final String KEY_SERIALIZER_KEY = "key.serializer";
    public static final String BOOTSTRAP_SERVER_KEY = "bootstrap.servers";
    public static final String ACKS_KEY = "acks";
    public static final String TYPE_KEY = "others";

    public static final String BOOTSTRAP_SERVER_FLUME_KEY = "bootstrapServers";
    public static final String BATCH_SIZE_FLUME_KEY = "batchSize";
    public static final String PREPROCESSOR = "preprocessor";


    public static final int DEFAULT_BATCH_SIZE = 100;
    public static final String DEFAULT_VALUE_SERIALIZER =
            "org.apache.kafka.common.serialization.StringSerializer";
    public static final String DEFAULT_KEY_SERIALIZER =
            "org.apache.kafka.common.serialization.StringSerializer";
    public static final String DEFAULT_ACKS = "1";
    public static final String DEFAULT_TYPE = "default";
    public static final String DEFAULT_TOPIC = "flume-default";


}
