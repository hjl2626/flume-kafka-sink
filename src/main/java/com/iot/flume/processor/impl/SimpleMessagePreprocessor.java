package com.iot.flume.processor.impl;

import com.iot.flume.processor.MessagePreprocessor;
import org.apache.flume.Context;
import org.apache.flume.Event;

/**
 * Created by hjl on 2016/8/26.
 */
public class SimpleMessagePreprocessor implements MessagePreprocessor {
    public String extractKey(Event event, Context context) {
        return null;
    }

    public String extractTopic(Event event, Context context) {
        return null;
    }

    public String transformMessage(Event event, Context context) {
        return null;
    }
}
