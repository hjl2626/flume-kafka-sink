/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.iot.flume.sink;

import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

@Deprecated
public class NewProducerKafkaSinkUtil {

  private static final Logger log =
          LoggerFactory.getLogger(NewProducerKafkaSinkUtil.class);

  public static Properties getKafkaProperties(Context context) {
    log.info("context={}",context.toString());
    Properties props =  generateDefaultKafkaProps();
    setKafkaProps(context, props);
    addDocumentedKafkaProps(context, props);
    return props;
  }

  /**
   * Some of the producer properties are especially important
   * We documented them and gave them a camel-case name to match Flume config
   * If user set these, we will override any existing parameters with these
   * settings.
   * Knowledge of which properties are documented is maintained here for now.
   * If this will become a maintenance issue we'll set a proper data structure.
   */
  private static void addDocumentedKafkaProps(Context context,
                                              Properties kafkaProps)
          throws ConfigurationException {
    String brokerList = context.getString(NewProducerKafkaSinkConstants
            .BROKER_LIST_FLUME_KEY);
    if (brokerList == null) {
      throw new ConfigurationException("brokerList must contain at least " +
              "one Kafka broker");
    }
    kafkaProps.put(NewProducerKafkaSinkConstants.BROKER_LIST_KEY, brokerList);

    String requiredKey = context.getString(
            NewProducerKafkaSinkConstants.REQUIRED_ACKS_FLUME_KEY);

    if (requiredKey != null ) {
      kafkaProps.put(NewProducerKafkaSinkConstants.REQUIRED_ACKS_KEY, requiredKey);
    }

    int batchSize = context.getInteger(
            NewProducerKafkaSinkConstants.BATCH_SIZE_FLUME_KEY, NewProducerKafkaSinkConstants.DEFAULT_BATCH_SIZE);
    kafkaProps.put(NewProducerKafkaSinkConstants.BATCH_SIZE_KEY, batchSize);

  }


  /**
   * Generate producer properties object with some defaults
   * @return
   */
  private static Properties generateDefaultKafkaProps() {
    Properties props = new Properties();
    props.put(NewProducerKafkaSinkConstants.VALUE_SERIALIZER_KEY,
            NewProducerKafkaSinkConstants.DEFAULT_VALUE_SERIALIZER);
    props.put(NewProducerKafkaSinkConstants.KEY_SERIALIZER_KEY,
            NewProducerKafkaSinkConstants.DEFAULT_KEY_SERIALIZER);
    props.put(NewProducerKafkaSinkConstants.REQUIRED_ACKS_KEY,
            NewProducerKafkaSinkConstants.DEFAULT_REQUIRED_ACKS);
    return props;
  }


  /**
   * Add all configuration parameters starting with "kafka"
   * to producer properties
   */
  private static void setKafkaProps(Context context, Properties kafkaProps) {

    Map<String,String> kafkaProperties =
            context.getSubProperties(NewProducerKafkaSinkConstants.PROPERTY_PREFIX);

    for (Map.Entry<String,String> prop : kafkaProperties.entrySet()) {

      kafkaProps.put(prop.getKey(), prop.getValue());
      if (log.isDebugEnabled()) {
        log.debug("Reading a Kafka Producer Property: key: "
                + prop.getKey() + ", value: " + prop.getValue());
      }
    }
  }
}
