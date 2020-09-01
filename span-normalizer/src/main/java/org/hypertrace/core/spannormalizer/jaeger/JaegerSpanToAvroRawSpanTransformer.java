package org.hypertrace.core.spannormalizer.jaeger;

import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.JOB_CONFIG;

import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.hypertrace.core.datamodel.RawSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JaegerSpanToAvroRawSpanTransformer implements
    Transformer<byte[], Span, KeyValue<byte[], RawSpan>> {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(JaegerSpanToAvroRawSpanTransformer.class);

  private JaegerSpanNormalizer converter;

  @Override
  public void init(ProcessorContext context) {
    Config jobConfig = (Config) context.appConfigs().get(JOB_CONFIG);
    converter = JaegerSpanNormalizer.get(jobConfig);
  }

  @Override
  public KeyValue<byte[], RawSpan> transform(byte[] key, Span value) {
    try {
      RawSpan rawSpan = converter.convert(value);
      if (null != rawSpan) {
        return new KeyValue<>(key, rawSpan);
      }
      return null;
    } catch (Exception e) {
      LOGGER.error("Error converting spans - ", e);
      return null;
    }
  }

  @Override
  public void close() {
  }
}
