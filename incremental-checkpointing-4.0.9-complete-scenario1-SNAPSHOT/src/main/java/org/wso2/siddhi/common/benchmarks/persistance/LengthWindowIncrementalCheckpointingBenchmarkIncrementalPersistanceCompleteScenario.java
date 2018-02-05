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

import org.HdrHistogram.Histogram;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.persistence.IncrementalFileSystemPersistenceStore;
import org.wso2.siddhi.core.util.persistence.PersistenceStore;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * The benchmark for incremental checkpointing persistance.
 */
public class LengthWindowIncrementalCheckpointingBenchmarkIncrementalPersistanceCompleteScenario {
    private static final Logger log = Logger.getLogger(
            LengthWindowIncrementalCheckpointingBenchmarkIncrementalPersistanceCompleteScenario.class);
    private static final Histogram histogram = new Histogram(2);
    private static long warmupPeriod = 0;
    private static long totalExperimentDuration = 0;
    private static long totalNumberOfTimesToCall = 0;
    private static int scenarioRounds = 0;
    private static long elapsedDuration;
    private static boolean warmupStarted;
    private static boolean warmupEnded;
    private static int fullRounds;

    public static void main(String[] args) {
        totalExperimentDuration = Long.parseLong(args[0]) * 60000;
        warmupPeriod = Long.parseLong(args[1]) * 60000;
        scenarioRounds = Integer.parseInt(args[2]);

        ApplicationState app = new ApplicationState();
        app.doSetup();
        long timeAtStartup = System.currentTimeMillis();
        int durationOfOneRoundInMilliseconds = (int) ((totalExperimentDuration - warmupPeriod) / scenarioRounds);
        app.setTimeAtStartup(timeAtStartup);
        while (elapsedDuration < totalExperimentDuration) {
            testMethod(app, durationOfOneRoundInMilliseconds);

            elapsedDuration = System.currentTimeMillis() - timeAtStartup;
            fullRounds++;
        }
        app.doTearDown();
    }

    /**
     * The state object for communicating with JMH method.
     */
    public static class ApplicationState {
        public SiddhiAppRuntime siddhiAppRuntime;
        public SiddhiManager siddhiManager;
        public InputHandler inputHandler;
        public final int inputEventCount = 10000;
        public String data;
        final int eventWindowSize = 20000;
        public long timeAtStartup;

        public String siddhiApp = "" +
                "@app:name('Test') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int, description string );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.length(" + eventWindowSize + ") " +
                "select symbol, price, sum(volume) as totalVol, description " +
                "insert all events into OutStream ";

        public void setTimeAtStartup(long timeAtStartup) {
            this.timeAtStartup = timeAtStartup;
        }

        public void doSetup() {
            log.info("Do Setup");
            log.info("Incremental persistence test 1 - length window query - performance");
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

        public void doTearDown() {
            try {
                log.info("----- Length Batch Window Incremental Checkpoint Benchmark - Complete Scenario -----");
                log.info("Total experiment duration (minutes) :" + (totalExperimentDuration / 60000));
                log.info("Total number of complete rounds :" + fullRounds);
                log.info("Total number of persistance calls :" + totalNumberOfTimesToCall);
                log.info("Total number of restore calls :" + fullRounds);
                log.info("Warm-up duration (minutes):" + (warmupPeriod / 60000));
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
            Date date = new Date();
            log.info("Experiment completed at :" + dateFormat.format(date));
        }
    }

    public static void testMethod(ApplicationState state, int durationOfOneRoundInMilliseconds) {
        if (!warmupStarted && !warmupEnded) {
            warmupStarted = true;
            log.info("Warm up started...");
        }

        while (elapsedDuration < warmupPeriod) {
            state.siddhiAppRuntime.persist();

            try {
                state.inputHandler.send(new Object[]{"IBM", 100.4f, 100, state.data});
                state.inputHandler.send(new Object[]{"WSO2", 200.4f, 100, state.data});
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }

            elapsedDuration = System.currentTimeMillis() - state.timeAtStartup;
        }

        if (warmupStarted && !warmupEnded && (elapsedDuration > warmupPeriod)) {
            warmupEnded = true;
            log.info("Warm up completed...");
        }

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        log.info("Started persisting round at :" + dateFormat.format(date));
        long threadSleep = 10;
        long numberOfTimesToCall = durationOfOneRoundInMilliseconds / threadSleep;
        totalNumberOfTimesToCall += numberOfTimesToCall;
        log.info("No of times to call:" + numberOfTimesToCall);
        long counter = 0;
        long firstTimeStamp = System.currentTimeMillis();
        while (counter < numberOfTimesToCall) {
            try {
                state.inputHandler.send(new Object[]{"IBM", 100.4f, 100, state.data});
                state.inputHandler.send(new Object[]{"WSO2", 200.4f, 100, state.data});

                state.siddhiAppRuntime.persist();

                try {
                    Thread.sleep(threadSleep);
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
            counter++;
        }

        dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        date = new Date();
        log.info("Completed persisting round and started restoration round at :" + dateFormat.format(date));

        try {
            state.siddhiAppRuntime.shutdown();
            state.siddhiAppRuntime = state.siddhiManager.createSiddhiAppRuntime(state.siddhiApp);
            state.inputHandler = state.siddhiAppRuntime.getInputHandler("StockStream");
            state.siddhiAppRuntime.start();
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }

        try {
            state.siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            log.error(e.getMessage(), e);
        }

        long secondTimeStamp = System.currentTimeMillis();
        long latency = secondTimeStamp - firstTimeStamp;
        histogram.recordValue(latency);

        dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        date = new Date();
        log.info("Completed restoration round at :" + dateFormat.format(date));
    }
}
