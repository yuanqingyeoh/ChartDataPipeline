/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package marketdata;

import marketdata.function.AggCandleFunction;
import marketdata.model.*;
import marketdata.model.candle.*;
import marketdata.trigger.CandleTrigger;
import marketdata.trigger.MyTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.mapping.Mapper;
import marketdata.function.TradeToCandleFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.*;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	private static final Logger LOG = LoggerFactory.getLogger(DataStreamJob.class);
	private static final String BROKERS = "kafka:9092";
// 	private static final String BROKERS = "localhost:29092";

	private static final String CASSANDRA_HOSTNAME = "cassandra";

	public static void main(String[] args) throws Exception {

		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		JsonDeserializationSchema<StreamInput> jsonFormat=new JsonDeserializationSchema<>(StreamInput.class);

		KafkaSource<StreamInput> source = KafkaSource.<StreamInput>builder()
				.setBootstrapServers(BROKERS)
				.setTopics("crypto-websocket-stream")
				.setGroupId("CRYPTO_SOCKET_CONSUMER_FLINK")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(jsonFormat)
				.build();

		DataStream<StreamInput> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
		DataStream<TickData> data_tick = input.flatMap(new FlatMapFunction<StreamInput, TickData>() {
			@Override
			public void flatMap(StreamInput streamInput, Collector<TickData> collector) throws Exception {
				streamInput.getResult().getData().forEach(dataItem -> {
					collector.collect(new TickData(dataItem.getI(), new Date(dataItem.getT()), new BigDecimal(dataItem.getP()), new BigDecimal(dataItem.getQ())));
				});
			}
		});

		data_tick.print("Received ");
		CassandraSink.addSink(data_tick)
				.setHost(CASSANDRA_HOSTNAME, 9042)
				.setDefaultKeyspace("market_data")
				.setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
				.build();

		// Processing M1
		DataStream<Candle> candleStream = data_tick
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<TickData>forBoundedOutOfOrderness(Duration.ofSeconds(1))
								.withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime())
								.withIdleness(Duration.ofSeconds(10)))
				.keyBy(TickData::getSymbol)
				.window(TumblingEventTimeWindows.of(Time.minutes(1)))
				.allowedLateness(Time.seconds(1))
				.trigger(MyTrigger.create())
				.process(new TradeToCandleFunction());

		DataStream<Candle_M1> data_m1 = candleStream.map(Candle_M1::new);

		data_m1.print("Processed M1 ");
		CassandraSink.addSink(data_m1)
				.setHost(CASSANDRA_HOSTNAME, 9042)
				.setDefaultKeyspace("market_data")
				.setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
				.build();


		// Processing M5
		candleStream = candleStream
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<Candle>forBoundedOutOfOrderness(Duration.ofSeconds(1))
								.withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime())
								.withIdleness(Duration.ofSeconds(10)))
				.keyBy(Candle::getSymbol)
				.window(TumblingEventTimeWindows.of(Time.minutes(5)))
				.trigger(CandleTrigger.create(Duration.ofMinutes(1)))
				.process(new AggCandleFunction());

		DataStream<Candle_M5> data_m5 = candleStream.map(Candle_M5::new);
		data_m5.print("Processed M5 ");
		CassandraSink.addSink(data_m5)
				.setHost(CASSANDRA_HOSTNAME, 9042)
				.setDefaultKeyspace("market_data")
				.setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
				.build();

		// Processing M15
		candleStream = candleStream
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<Candle>forBoundedOutOfOrderness(Duration.ofSeconds(1))
								.withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime())
								.withIdleness(Duration.ofSeconds(10)))
				.keyBy(Candle::getSymbol)
				.window(TumblingEventTimeWindows.of(Time.minutes(15)))
				.trigger(CandleTrigger.create(Duration.ofMinutes(5)))
				.process(new AggCandleFunction());

		DataStream<Candle_M15> data_m15 = candleStream.map(Candle_M15::new);
		data_m15.print("Processed M15 ");
		CassandraSink.addSink(data_m15)
				.setHost(CASSANDRA_HOSTNAME, 9042)
				.setDefaultKeyspace("market_data")
				.setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
				.build();

		// Processing M30
		candleStream = candleStream
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<Candle>forBoundedOutOfOrderness(Duration.ofSeconds(1))
								.withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime())
								.withIdleness(Duration.ofSeconds(10)))
				.keyBy(Candle::getSymbol)
				.window(TumblingEventTimeWindows.of(Time.minutes(30)))
				.trigger(CandleTrigger.create(Duration.ofMinutes(15)))
				.process(new AggCandleFunction());

		DataStream<Candle_M30> data_m30 = candleStream.map(Candle_M30::new);
		data_m30.print("Processed M30 ");
		CassandraSink.addSink(data_m30)
				.setHost(CASSANDRA_HOSTNAME, 9042)
				.setDefaultKeyspace("market_data")
				.setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
				.build();

		// Processing H1
		candleStream = candleStream
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<Candle>forBoundedOutOfOrderness(Duration.ofSeconds(1))
								.withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime())
								.withIdleness(Duration.ofSeconds(10)))
				.keyBy(Candle::getSymbol)
				.window(TumblingEventTimeWindows.of(Time.hours(1)))
				.trigger(CandleTrigger.create(Duration.ofMinutes(30)))
				.process(new AggCandleFunction());

		DataStream<Candle_H1> data_h1 = candleStream.map(Candle_H1::new);
		data_h1.print("Processed M30 ");
		CassandraSink.addSink(data_h1)
				.setHost(CASSANDRA_HOSTNAME, 9042)
				.setDefaultKeyspace("market_data")
				.setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
				.build();

		// Processing H4
		candleStream = candleStream
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<Candle>forBoundedOutOfOrderness(Duration.ofSeconds(1))
								.withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime())
								.withIdleness(Duration.ofSeconds(10)))
				.keyBy(Candle::getSymbol)
				.window(TumblingEventTimeWindows.of(Time.hours(4)))
				.trigger(CandleTrigger.create(Duration.ofHours(1)))
				.process(new AggCandleFunction());

		DataStream<Candle_H4> data_h4 = candleStream.map(Candle_H4::new);
		data_h4.print("Processed H4 ");
		CassandraSink.addSink(data_h4)
				.setHost(CASSANDRA_HOSTNAME, 9042)
				.setDefaultKeyspace("market_data")
				.setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
				.build();

		// Processing D
		candleStream = candleStream
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<Candle>forBoundedOutOfOrderness(Duration.ofSeconds(1))
								.withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime())
								.withIdleness(Duration.ofSeconds(10)))
				.keyBy(Candle::getSymbol)
				.window(TumblingEventTimeWindows.of(Time.days(1)))
				.trigger(CandleTrigger.create(Duration.ofHours(4)))
				.process(new AggCandleFunction());

		DataStream<Candle_D> data_d = candleStream.map(Candle_D::new);
		data_d.print("Processed D ");
		CassandraSink.addSink(data_d)
				.setHost(CASSANDRA_HOSTNAME, 9042)
				.setDefaultKeyspace("market_data")
				.setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
				.build();

		System.out.println(env.getExecutionPlan());
		// Execute program, beginning computation.
		env.execute("Crypto Trade Data Stream Job");
	}
}
