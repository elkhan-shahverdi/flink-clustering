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

package com.pinvity.clustering;

import com.pinvity.clustering.model.Centroid;
import com.pinvity.clustering.model.Point;
import com.pinvity.clustering.repository.DBHandlerMongo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.CollectionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.log4j.Logger;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;


public class KMeans  implements  Runnable {

    private final static Logger logger = Logger.getLogger(KMeans.class);
    public static int totalDuration = 0;
    int k;

    CountDownLatch          countDownLatch;
    DataSet<Point>          points;
    DataSet<Centroid>       centroids;
    ExecutionEnvironment    env;

    public KMeans(int k, List<Point> points, List<Centroid> centroids, CountDownLatch countDownLatch){
        this.env            = new CollectionEnvironment();
        this.k              = k;
        this.points         = env.fromCollection(points);
        this.centroids      = env.fromCollection(centroids);
        this.countDownLatch = countDownLatch;
    }



    public void run() {
        LocalDateTime startTime = LocalDateTime.now();
        logger.info("Thread["+k+"] start to clustering");
        IterativeDataSet<Centroid> loop = centroids.iterate(10);

        logger.info("Thread["+k+"] compute new centroids from point counts and coordinate sums");
        DataSet<Centroid> newCentroids = points
                // compute closest centroid for each point
                .map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
                // count and sum point coordinates for each centroid
                .map(new CountAppender())
                .groupBy(0).reduce(new CentroidAccumulator())
                // compute new centroids from point counts and coordinate sums
                .map(new CentroidAverager());

        // feed new centroids back into next iteration
        logger.info("Thread["+k+"] feed new centroids back into next iteration");
        DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);

        try {
            DBHandlerMongo.getInstance().insertClusters(finalCentroids.collect(),k);
        } catch (Exception e) {
            logger.info(e.getMessage(),e.getCause());
            e.printStackTrace();
            System.exit(0);
        }
        logger.info("Thread["+k+"] finished clustering");

        LocalDateTime endTime = LocalDateTime.now();
        totalDuration += startTime.until(endTime, ChronoUnit.SECONDS);
        logger.info("Thread["+k+"] Duration: "+ startTime.until(endTime, ChronoUnit.SECONDS));
        countDownLatch.countDown();
    }



    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /**
     * Determines the closest cluster center for a data point.
     */
    /** Determines the closest cluster center for a data point. */
    @ForwardedFields("*->1")
    public static final class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Integer, Point>> {

        private Collection<Centroid> centroids;

        /** Reads the centroid values from a broadcast variable into a collection. */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple2<Integer, Point> map(Point p) throws Exception {

            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;

            // check all cluster centers
            for (Centroid centroid : centroids) {
                // compute distance
                double distance = p.haversineDistance(centroid);

                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.id;
                }
            }

            // emit a new record with the center id and the data point.
            return new Tuple2<>(closestCentroidId, p);
        }
    }

    /** Appends a count variable to the tuple. */
    @ForwardedFields("f0;f1")
    public static final class CountAppender implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> t) {
            return new Tuple3<>(t.f0, t.f1, 1L);
        }
    }

    /** Sums and counts point coordinates. */
    @ForwardedFields("0")
    public static final class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
            return new Tuple3<>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
        }
    }

    /** Computes new centroid from coordinate sum and count of points. */
    @ForwardedFields("0->id")
    public static final class CentroidAverager implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {

        @Override
        public Centroid map(Tuple3<Integer, Point, Long> value) {
            return new Centroid(value.f0, value.f1.div(value.f2),value.f2);
        }
    }
}
