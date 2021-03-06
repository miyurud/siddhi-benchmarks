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
import org.wso2.siddhi.core.util.persistence.PersistenceStore;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * The benchmark for incremental checkpointing pre-persistance.
 */
@SuppressWarnings("unused")
@SuppressFBWarnings("UUF_UNUSED_FIELD")
public class LengthWindowIncrementalCheckpointingBenchmarkIncrementalPrePersistance {
    private static final Logger log = Logger.getLogger(
            LengthWindowIncrementalCheckpointingBenchmarkIncrementalPrePersistance.class);
    private static double totalLatencies;
    private static final Histogram histogram = new Histogram(2);
    private static long warmupPeriod = 0;
    private static long totalExperimentDuration = 0;
    private static long elapsedDuration;
    private static boolean warmupStarted;
    private static boolean warmupEnded;

    public static void main(String[] args) {
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
        final int eventWindowSize = 1000;
        public String data;

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

            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

            inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
            siddhiAppRuntime.start();

            data = randomAlphaNumeric(128);

            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            //get current date time with Date()
            Date date = new Date();
            log.info("Experiment started at : " + dateFormat.format(date));
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
                log.info("Do TearDown");

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
        for (int i = 0; i < state.inputEventCount; i++) {
            try {
                state.inputHandler.send(new Object[]{"IBM", 75.6f + i, 100, state.data});
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}
