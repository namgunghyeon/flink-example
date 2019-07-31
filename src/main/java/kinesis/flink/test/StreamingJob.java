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

package kinesis.flink.test;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.util.Collector;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(StreamingJob.class);
    private static final String region = "ap-northeast-1";
    private static final String inputStreamName = "TextInputStream2";
    private static final String outputStreamName = "WordCountOutputStream2";

    private static DataStream createSourceFromStaticConfig(
            StreamExecutionEnvironment env) {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "");
        inputProperties.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "");
        //inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION,
        //        "LATEST");
        inputProperties.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS, "1000");

        return env.addSource(new FlinkKinesisConsumer(inputStreamName,
                new MovieSchema(), inputProperties));
    }

    private static FlinkKinesisProducer createSinkFromStaticConfig() {
        Properties outputProperties = new Properties();
        outputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        outputProperties.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "");
        outputProperties.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "");


        FlinkKinesisProducer sink = new FlinkKinesisProducer(new
                SimpleStringSchema(), outputProperties);
        sink.setDefaultStream(outputStreamName);
        sink.setDefaultPartition("0");
        return sink;
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream input = createSourceFromStaticConfig(env);
        input.filter(movie -> MovieData.class.isAssignableFrom(movie.getClass()))
                .map(new MovieDataRecordMapper())
                .keyBy(0, 1)
                .timeWindow(Time.minutes(1))
                .aggregate(new MovieDataAggregateFunction())
                .map(new MapFunction<Tuple4<String, String, Integer, Long>, String>() {
                    @Override
                    public String map(Tuple4<String, String, Integer, Long> value) throws Exception {
                        Gson gson = new Gson();
                        String result = gson.toJson(new AggregatedMovieData(value.f0, value.f1, value.f3, value.f2, "movie_userid"));
                        LOG.info(result);

                        return result;
                    }
                })
                .addSink(createSinkFromStaticConfig());


        DataStream input2 = createSourceFromStaticConfig(env);
        input2.filter(movie -> MovieData.class.isAssignableFrom(movie.getClass()))
                .map(new MovieDataRecordMapper())
                .keyBy(0)
                .timeWindow(Time.minutes(1))
                .aggregate(new MovieDataAggregateFunction())
                .map(new MapFunction<Tuple4<String, String, Integer, Long>, String>() {
                    @Override
                    public String map(Tuple4<String, String, Integer, Long> value) throws Exception {
                        Gson gson = new Gson();
                        String result = gson.toJson(new AggregatedMovieData(value.f0, value.f1, value.f3, value.f2,  "movie"));
                        LOG.info(result);

                        return result;
                    }
                })
                .addSink(createSinkFromStaticConfig());

        env.execute("Test");
    }

    private static class AggregatedMovieData {
        private final  String movieId;
        private final String userId;
        private final String aggType;
        private final Long timestamp;
        private final Integer count;

        public AggregatedMovieData(String movieId, String userId, Long timestamp, Integer count, String aggType) {
            this.movieId = movieId;
            this.userId = userId;
            this.timestamp = timestamp;
            this.count = count;
            this.aggType = aggType;
        }

        public String getMovieId() {
            return movieId;
        }

        public String getUserId() {
            return userId;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public Integer getCount() {
            return count;
        }

        public String getAggType() {
            return aggType;
        }
    }


    private static class MovieDataRecordMapper implements MapFunction<Movie, Tuple3<String, String, Long>> {

        @Override
        public Tuple3<String, String, Long> map(Movie record) {
            MovieData movieData = (MovieData) record;
            return Tuple3.of(
                    movieData.getMoveId(),
                    movieData.getUsrId(),
                    movieData.getTime());
        }
    }

    private static class MovieDataAggregateFunction implements AggregateFunction<Tuple3<String, String, Long>,
            Tuple4<String, String, Integer, Long>, Tuple4<String, String, Integer, Long>> {

        @Override
        public Tuple4<String, String, Integer, Long> createAccumulator() {
            return new Tuple4<>(null, null, 0, 0L);
        }

        @Override
        public Tuple4<String, String, Integer, Long> add(Tuple3<String, String, Long> record,
                                                         Tuple4<String, String, Integer, Long> movieAggregate) {
            movieAggregate.f0 = record.f0;
            movieAggregate.f1 = record.f1;
            movieAggregate.f2 += 1;
            movieAggregate.f3 = record.f2;

            return movieAggregate;
        }

        @Override
        public Tuple4<String, String, Integer, Long> getResult(Tuple4<String, String, Integer, Long> movieAggregate) {
            return movieAggregate;
        }

        @Override
        public Tuple4<String, String, Integer, Long> merge(Tuple4<String, String, Integer, Long> acc1,
                                                           Tuple4<String, String, Integer, Long> acc2) {
            acc1.f2 += acc2.f2;
            if (acc1.f3 < acc2.f3) {
                acc1.f3 = acc2.f3;
            }
            return acc1;
        }
    }

    private static class MovieDataTimestampAssigner extends AscendingTimestampExtractor<MovieData> {

        @Override
        public long extractAscendingTimestamp(MovieData record) {
            return record.getTime();
        }
    }
}
