package org.hypertrace.core.spannormalizer;

import com.typesafe.config.*;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.kafkastreams.framework.serdes.AvroSerde;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanNormalizer;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanSerde;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanToAvroRawSpanTransformer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class KafkaConsumerTest {

    @Test
    public void kafkaConsumer() throws Exception {
        KafkaConsumer<byte[], JaegerSpanInternalModel.Span> consumer =
                new KafkaConsumer<>(getConsumerProps(), new ByteArrayDeserializer(), new JaegerSpanSerde.De());

        JaegerSpanNormalizer normalizer = new JaegerSpanNormalizer();

        try {
            consumer.subscribe(Collections.singletonList("jaeger-spans"));

            while (true) {
                ConsumerRecords<byte[], JaegerSpanInternalModel.Span> records = consumer.poll(100);
                for (ConsumerRecord<byte[], JaegerSpanInternalModel.Span> record : records) {
                    System.out.println("key: " + record.key());
                    System.out.println("value: " + record.value());
                    JaegerSpanInternalModel.Span jaegerSpan = record.value();
                    RawSpan rawSpan = normalizer.convert(jaegerSpan);
                    System.out.println("raw span: " + rawSpan);
                    System.out.println("--------------");
                }
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
        }
    }

    private Properties getConsumerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    class RawSpanDeserializer implements Deserializer<RawSpan> {

        @Override
        public RawSpan deserialize(String topic, byte[] data) {
//            AvroSerde
            SpecificDatumReader<RawSpan> datumReader = new SpecificDatumReader<>(RawSpan.getClassSchema());
            ByteArrayInputStream is = new ByteArrayInputStream(data);
            BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(is, null);
            try {
                return datumReader.read(null, binaryDecoder);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }
    }

}
