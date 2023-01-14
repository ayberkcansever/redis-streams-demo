package com.canseverayberk.redisstreams.config;

import com.canseverayberk.redisstreams.bean.DemoStreamListener;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandKeyword;
import io.lettuce.core.protocol.CommandType;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.scheduling.annotation.Async;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Random;

@Configuration
@RequiredArgsConstructor
public class StreamsConfiguration {

    private final RedisConnectionFactory redisConnectionFactory;
    private final RedisTemplate<String, String> redisTemplate;
    private final DemoStreamListener demoStreamListener;

    @Value("${stream.name}")
    private String streamName;

    @Value("${stream.group-prefix}")
    private String streamGroupPrefix;

    @Value("${stream.group-count}")
    private int streamGroupCount;

    @Async
    @PostConstruct
    public void init() throws UnknownHostException {
        for (int i = 0; i < streamGroupCount; i++) {
            int group = new Random().nextInt(0, 10000);
            ensureGroupNameCreated(redisTemplate, group);
            createSubscription(redisConnectionFactory, demoStreamListener, group);
        }
    }

    private void createSubscription(RedisConnectionFactory redisConnectionFactory, DemoStreamListener demoStreamListener, int group) throws UnknownHostException {
        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, ObjectRecord<String, String>> options = StreamMessageListenerContainer
                .StreamMessageListenerContainerOptions
                .builder()
                .pollTimeout(Duration.ofMillis(100))
                .targetType(String.class)
                .build();

        StreamMessageListenerContainer<String, ObjectRecord<String, String>>  listenerContainer = StreamMessageListenerContainer
                .create(redisConnectionFactory, options);

        listenerContainer.receiveAutoAck(
                Consumer.from(streamGroupPrefix + "-" + group, InetAddress.getLocalHost().getHostName()),
                StreamOffset.create(streamName, ReadOffset.lastConsumed()),
                demoStreamListener);
        listenerContainer.start();
    }

    private void ensureGroupNameCreated(RedisTemplate<String, String> redisTemplate, int group) {
        try {
            RedisAsyncCommands commands = (RedisAsyncCommands) redisTemplate
                    .getConnectionFactory()
                    .getConnection()
                    .getNativeConnection();

            CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                    .add(CommandKeyword.CREATE)
                    .add(streamName)
                    .add(streamGroupPrefix + "-" + group)
                    .add("$")
                    .add("MKSTREAM");

            commands.dispatch(CommandType.XGROUP, new StatusOutput<>(StringCodec.UTF8), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
