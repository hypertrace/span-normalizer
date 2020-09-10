package org.hypertrace.core.spannormalizer;

import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.INPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.JOB_CONFIG;
import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.KAFKA_STREAMS_CONFIG_KEY;
import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.OUTPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.SCHEMA_REGISTRY_CONFIG_KEY;
import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.SPAN_TYPE_CONFIG_KEY;

import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.kafkastreams.framework.serdes.SchemaRegistryBasedAvroSerde;
import org.hypertrace.core.kafkastreams.framework.timestampextractors.UseWallclockTimeOnInvalidTimestamp;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanSerde;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanToAvroRawSpanTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpanNormalizer extends KafkaStreamsApp {

  private static final Logger logger = LoggerFactory.getLogger(SpanNormalizer.class);

  private Map<String, String> schemaRegistryConfig;

  public SpanNormalizer(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  public StreamsBuilder buildTopology(Properties properties, StreamsBuilder streamsBuilder, Map<String, KStream<?, ?>> inputStreams) {
    SchemaRegistryBasedAvroSerde<RawSpan> rawSpanSerde = new SchemaRegistryBasedAvroSerde<>(
            RawSpan.class);
    rawSpanSerde.configure(schemaRegistryConfig, false);

    String inputTopic = properties.getProperty(INPUT_TOPIC_CONFIG_KEY);
    String outputTopic = properties.getProperty(OUTPUT_TOPIC_CONFIG_KEY);

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
  public Properties getStreamsConfig(Config config) {
    Properties properties = new Properties();

    schemaRegistryConfig = ConfigUtils.getFlatMapConfig(config, SCHEMA_REGISTRY_CONFIG_KEY);
    properties.putAll(schemaRegistryConfig);

    properties.put(SPAN_TYPE_CONFIG_KEY, config.getString(SPAN_TYPE_CONFIG_KEY));
    properties.put(INPUT_TOPIC_CONFIG_KEY, config.getString(INPUT_TOPIC_CONFIG_KEY));
    properties.put(OUTPUT_TOPIC_CONFIG_KEY, config.getString(OUTPUT_TOPIC_CONFIG_KEY));
    properties.putAll(ConfigUtils.getFlatMapConfig(config, KAFKA_STREAMS_CONFIG_KEY));

    properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
            UseWallclockTimeOnInvalidTimestamp.class);
    properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
            LogAndContinueExceptionHandler.class);

    if (config.hasPath(PRE_CREATE_TOPICS)) {
      properties.put(PRE_CREATE_TOPICS, config.getString(PRE_CREATE_TOPICS));
    }

    properties.put(JOB_CONFIG, config);

    return properties;
  }

  @Override
  public Logger getLogger() {
    return logger;
  }

  @Override
  public List<String> getInputTopics(Properties properties) {
    return Arrays.asList(properties.getProperty(INPUT_TOPIC_CONFIG_KEY));
  }

  @Override
  public List<String> getOutputTopics(Properties properties) {
    return Arrays.asList(properties.getProperty(OUTPUT_TOPIC_CONFIG_KEY));
  }
}
