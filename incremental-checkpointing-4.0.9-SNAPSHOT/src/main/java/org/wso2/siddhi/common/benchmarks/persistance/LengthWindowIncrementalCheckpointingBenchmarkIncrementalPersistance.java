/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.common.benchmarks.persistance;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.HdrHistogram.Histogram;
import org.apache.log4j.Logger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.persistence.IncrementalFileSystemPersistenceStore;
import org.wso2.siddhi.core.util.persistence.PersistenceStore;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * The benchmark for incremental checkpointing persistance.
 */
@SuppressWarnings("unused")
@SuppressFBWarnings("UUF_UNUSED_FIELD")
public class LengthWindowIncrementalCheckpointingBenchmarkIncrementalPersistance {
    private static final Logger log = Logger.getLogger(
            LengthWindowIncrementalCheckpointingBenchmarkIncrementalPersistance.class);
    private static double totalLatencies;
    private static final Histogram histogram = new Histogram(2);
    private static long warmupPeriod = 0;
    private static long totalExperimentDuration = 0;
    private static long elapsedDuration;
    private static boolean warmupStarted;
    private static boolean warmupEnded;

    public static void main(String[] args) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        log.info("Experiment started at :" + dateFormat.format(date));
        totalExperimentDuration = Long.parseLong(args[0]) * 60000;
        warmupPeriod = Long.parseLong(args[1]) * 60000;

        ApplicationState app = new ApplicationState();
        app.doSetup();
        long timeAtStartup = System.currentTimeMillis();

        while (elapsedDuration < totalExperimentDuration) {
            testMethod(app);
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
            elapsedDuration = System.currentTimeMillis() - timeAtStartup;
        }
        app.doTearDown();
    }

    /**
     * The state object for communicating with JMH method.
     */
    @State(Scope.Thread)
    public static class ApplicationState {
        public SiddhiAppRuntime siddhiAppRuntime;
        public SiddhiManager siddhiManager;
        public InputHandler inputHandler;
        public final int inputEventCount = 10000;
        public String data;
        final int eventWindowSize = 10;

        public String siddhiApp = "" +
                "@app:name('Test') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int, description string );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.length(" + eventWindowSize + ") " +
                "select symbol, price, sum(volume) as totalVol, description " +
                "insert all events into OutStream ";

        @Setup(Level.Trial)
        public void doSetup() {
            log.info("Do Setup");
            log.info("Incremental persistence test 1 - length window query - performance");
            //PersistenceStore persistenceStore = new InMemoryPersistenceStore();
            PersistenceStore persistenceStore = new org.wso2.siddhi.core.util.persistence.FileSystemPersistenceStore();

            siddhiManager = new SiddhiManager();
            siddhiManager.setPersistenceStore(persistenceStore);
            siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore());



            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

            inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
            siddhiAppRuntime.start();

            data = randomAlphaNumeric(128);

            for (int i = 0; i < inputEventCount; i++) {
                try {
                    inputHandler.send(new Object[]{"IBM", 75.6f + i, 100, data});
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
            }
            try {
                Thread.sleep(100);

                //persisting
                siddhiAppRuntime.persist();
                Thread.sleep(5000);

                inputHandler.send(new Object[]{"IBM", 100.4f, 100, data});
                inputHandler.send(new Object[]{"WSO2", 200.4f, 100, data});

                inputHandler.send(new Object[]{"IBM", 300.4f, 100, data});
                inputHandler.send(new Object[]{"WSO2", 400.4f, 200, data});
                Thread.sleep(100);

                //persisting
                siddhiAppRuntime.persist();
                Thread.sleep(5000);

                DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                //get current date time with Date()
                Date date = new Date();
                log.info("Experiment started at : " + dateFormat.format(date));

            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }

        public static String randomAlphaNumeric(int count) {
            final String alphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            StringBuilder builder = new StringBuilder();

            while (count-- != 0) {
                int character = (int) (Math.random() * alphaNumericString.length());

                builder.append(alphaNumericString.charAt(character));
            }

            return builder.toString();
        }

        @TearDown(Level.Trial)
        public void doTearDown() {
            try {
                log.info("----- Length Batch Window Incremental Checkpoint Benchmark - Persistance -----");
                log.info("Total experiment duration (minutes) :" + (totalExperimentDuration / 60000));
                log.info("Warmup duration (minutes):" + (warmupPeriod / 60000));
                log.info("Number of recordings :" + histogram.getTotalCount());
                log.info("Average Latency (ms) :" + (histogram.getMean()));
                log.info("Percentile Latencies (ms) (90):" + histogram.getValueAtPercentile(90) + ", (95):" + histogram
                        .getValueAtPercentile(95) + ", (99):" + histogram.getValueAtPercentile(99));
                log.info("Latency Standard Deviation (ms) :" + histogram.getStdDeviation());
                log.info("Latency Max (ms) :" + histogram.getMaxValue());
                siddhiAppRuntime.shutdown();
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }

            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            //get current date time with Date()
            Date date = new Date();
            log.info("Experiment completed at : " + dateFormat.format(date));
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.SampleTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public static void testMethod(ApplicationState state) {
        try {
            state.inputHandler.send(new Object[]{"IBM", 100.4f, 100, state.data});
            state.inputHandler.send(new Object[]{"WSO2", 200.4f, 100, state.data});
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }

        if (elapsedDuration > warmupPeriod) {
            long firstTimeStamp = System.currentTimeMillis();
            state.siddhiAppRuntime.persist();
            long secondTimeStamp = System.currentTimeMillis();
            long latency = secondTimeStamp - firstTimeStamp;
            histogram.recordValue(latency);

            totalLatencies += latency;
        } else {
            state.siddhiAppRuntime.persist();
        }

        if (!warmupStarted && !warmupEnded) {
            warmupStarted = true;
            log.info("Warm up started...");
        }

        if (warmupStarted && !warmupEnded && (elapsedDuration > warmupPeriod)) {
            warmupEnded = true;
            log.info("Warm up completed...");
        }
    }
}
