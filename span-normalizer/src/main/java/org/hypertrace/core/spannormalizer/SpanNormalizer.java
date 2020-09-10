package org.hypertrace.core.spannormalizer;

import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.INPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.KAFKA_STREAMS_CONFIG_KEY;
import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.OUTPUT_TOPIC_CONFIG_KEY;

import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.kafkastreams.framework.serdes.SchemaRegistryBasedAvroSerde;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanSerde;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanToAvroRawSpanTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpanNormalizer extends KafkaStreamsApp {

  private static final Logger logger = LoggerFactory.getLogger(SpanNormalizer.class);

  public SpanNormalizer(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  public StreamsBuilder buildTopology(Map<String, Object> streamsProperties,
      StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> inputStreams) {
    SchemaRegistryBasedAvroSerde<RawSpan> rawSpanSerde = new SchemaRegistryBasedAvroSerde<>(
        RawSpan.class);

    rawSpanSerde.configure(streamsProperties, false);

    String inputTopic = getAppConfig().getString(INPUT_TOPIC_CONFIG_KEY);
    String outputTopic = getAppConfig().getString(OUTPUT_TOPIC_CONFIG_KEY);

    KStream<byte[], Span> inputStream = (KStream<byte[], Span>) inputStreams.get(inputTopic);
    if (inputStream == null) {
      inputStream = streamsBuilder
          .stream(inputTopic, Consumed.with(Serdes.ByteArray(), new JaegerSpanSerde()));
      inputStreams.put(inputTopic, inputStream);
    }

    inputStream
        .transform(
            JaegerSpanToAvroRawSpanTransformer::new)
        .to(outputTopic,
            Produced.with(Serdes.String(), Serdes.serdeFrom(rawSpanSerde, rawSpanSerde)));

    return streamsBuilder;
  }

  @Override
  public Map<String, Object> getStreamsConfig(Config jobConfig) {
    Map<String, Object> streamsConfig = new HashMap<>(
        ConfigUtils.getFlatMapConfig(jobConfig, KAFKA_STREAMS_CONFIG_KEY));
    return streamsConfig;
  }

  @Override
  public Logger getLogger() {
    return logger;
  }
}
