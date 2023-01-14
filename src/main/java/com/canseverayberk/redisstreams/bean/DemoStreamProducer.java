package com.canseverayberk.redisstreams.bean;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class DemoStreamProducer {

    private final RedisTemplate<String, String> redisTemplate;

    @Value("${stream.name}")
    private String streamName;

    @Scheduled(fixedDelay = 1000)
    public void produce() {
        long currentTimeMillis = System.currentTimeMillis();
        redisTemplate
                .getConnectionFactory()
                .getConnection()
                .streamCommands().xAdd(streamName.getBytes(), Map.of("message".getBytes(), (currentTimeMillis + " - Hi there!").getBytes()));
        log.info("Produced: {}", currentTimeMillis);
    }
}
