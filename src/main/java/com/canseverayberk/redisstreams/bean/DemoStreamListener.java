package com.canseverayberk.redisstreams.bean;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DemoStreamListener implements StreamListener<String, ObjectRecord<String, String>> {

    @Override
    public void onMessage(ObjectRecord<String, String> record) {
        log.info(record.getValue());
    }

}