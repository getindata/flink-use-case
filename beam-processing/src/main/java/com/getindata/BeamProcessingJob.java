package com.getindata;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.Collections;
import java.util.stream.StreamSupport;

public class BeamProcessingJob {
    public static void main(String[] args) {

        final Conf conf = new Conf(args);
        final PipelineOptions options = PipelineOptionsFactory.create();
        final Pipeline pipeline = Pipeline.create(options);

        pipeline.getCoderRegistry().registerCoder(UserEvent.class, UserEventCoder.class);
        pipeline.apply(KafkaIO.read()
                .withTopics(Collections.singletonList(conf.getTopic()))
                .withBootstrapServers(conf.getKafkaBroker())
                .withValueCoder(new EventCoder())
                .withTimestampFn(input -> new Instant(input.getValue().timestamp()))
                .withWatermarkFn(input -> {
                    if (input.getValue().isWatermark())
                        return new Instant(input.getValue().timestamp());
                    else
                        //TODO some smarter watermark
                        return new Instant(input.getValue().timestamp());
                })
                .withoutMetadata())
                .apply(Window.into(Sessions.withGapDuration(Duration.standardSeconds(conf.getSessionGap()))))
                .apply("FormKeyValue", MapElements.via(new SimpleFunction<KV<byte[], Event>, KV<String, UserEvent>>() {
                    @Override
                    public KV<String, UserEvent> apply(KV<byte[], Event> input) {
                        final UserEvent value = (UserEvent) input.getValue();
                        return KV.of(value.userId(), value);
                    }
                }))
                .apply("GroupByUser", GroupByKey.create())
                .apply("CalculateSessionStatistics",
                        MapElements.via(new SimpleFunction<KV<String, Iterable<UserEvent>>, String>() {
                            @Override
                            public String apply(KV<String, Iterable<UserEvent>> input) {
                                final long end = StreamSupport.stream(input.getValue().spliterator(),
                                        false).mapToLong(UserEvent::timestamp).max().getAsLong();
                                final long start = StreamSupport.stream(input.getValue().spliterator(),
                                        false).mapToLong(UserEvent::timestamp).min().getAsLong();
                                final long count = StreamSupport.stream(input.getValue().spliterator(),
                                        false).filter(e -> e instanceof SongEvent).count();
                                return "User " + input.getKey() + " session took " + ((end - start) / 1000) + " " +
                                        "seconds and " + count + " songs.";
                            }
                        }))
                .apply(KafkaIO.write()
                        .withTopic(conf.getWriteTopic())
                        .withBootstrapServers(conf.getKafkaBroker())
                        .withValueCoder(StringUtf8Coder.of())
                        .values()
                );

        pipeline.run().waitUntilFinish();
    }
}


