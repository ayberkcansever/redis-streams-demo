package com.canseverayberk.redisstreams.bean;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.stream.ByteRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.lettuce.core.internal.LettuceLists.newList;
import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
@Component
@RequiredArgsConstructor
public class DemoStreamProducer {

    private final RedisTemplate<String, String> redisTemplate;

    @Value("${stream.name}")
    private String streamName;

    @Scheduled(fixedDelay = 1000)
    public void produce() {
        RedisConnection connection = getRedisConnection();

        RecordId recordId = connection.streamCommands().xAdd(streamName.getBytes(), Map.of("message".getBytes(), "Hi there!".getBytes()));
        Long streamLength = connection.streamCommands().xLen(streamName.getBytes());
        log.info("Produced: {}, length: {}", recordId.getValue(), streamLength);
    }

    @Scheduled(fixedDelay = 5000)
    public void xRange() {
        RedisConnection connection = getRedisConnection();

        long start = System.currentTimeMillis() - 2000;
        long end = System.currentTimeMillis();
        Range<String> range = Range.closed(start + "-0", end + "-0");

        List<ByteRecord> byteRecords = connection.streamCommands().xRange(streamName.getBytes(), range);
        String rangeRecords = byteRecords.stream()
                .map(byteRecord -> byteRecord.getId().getValue() + " --> " + new String(newList(byteRecord.getValue().values()).get(0), UTF_8))
                .collect(Collectors.joining("||"));
        log.info("Range {} - {} records: {}", start, end, rangeRecords);
    }

    @Scheduled(fixedDelay = 30000)
    public void xTrim() {
        RedisConnection connection = getRedisConnection();
        connection.streamCommands().xTrim(streamName.getBytes(), 20);
        // connection.streamCommands().xTrim(streamName.getBytes(), 20, true); // for better performance
        Long streamLength = connection.streamCommands().xLen(streamName.getBytes());
        log.info("Stream: {} is trimmed new length: {}", streamName, streamLength);
    }

    private RedisConnection getRedisConnection() {
        return redisTemplate
                .getConnectionFactory()
                .getConnection();
    }
}
