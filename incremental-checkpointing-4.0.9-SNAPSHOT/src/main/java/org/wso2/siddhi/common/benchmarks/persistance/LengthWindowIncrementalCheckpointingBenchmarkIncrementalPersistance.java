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

import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.persistence.IncrementalFileSystemPersistenceStore;
import org.wso2.siddhi.core.util.persistence.PersistenceStore;

public class LengthWindowIncrementalCheckpointingBenchmarkIncrementalPersistance {
    private static final Logger log = Logger.getLogger(LengthWindowIncrementalCheckpointingBenchmarkIncrementalPersistance.class);

    @State(Scope.Thread)
    public static class ApplicationState {
        public SiddhiAppRuntime siddhiAppRuntime;
        public SiddhiManager siddhiManager;
        public InputHandler inputHandler;
        final int inputEventCount = 10000;
        final int eventWindowSize = 4;

        public String siddhiApp = "" +
                "@app:name('Test') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.length(" + eventWindowSize + ") " +
                "select symbol, price, sum(volume) as totalVol " +
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

            for (int i = 0; i < inputEventCount; i++) {
                try {
                    inputHandler.send(new Object[]{"IBM", 75.6f + i, 100});
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            try {
                Thread.sleep(100);

                //persisting
                siddhiAppRuntime.persist();
                Thread.sleep(5000);

                inputHandler.send(new Object[]{"IBM", 100.4f, 100});
                //Thread.sleep(100);
                inputHandler.send(new Object[]{"WSO2", 200.4f, 100});

                inputHandler.send(new Object[]{"IBM", 300.4f, 100});
                //Thread.sleep(100);
                inputHandler.send(new Object[]{"WSO2", 400.4f, 200});
                Thread.sleep(100);

                //persisting
                siddhiAppRuntime.persist();
                Thread.sleep(5000);

                inputHandler.send(new Object[]{"IBM", 100.4f, 100});
                //Thread.sleep(100);
                inputHandler.send(new Object[]{"WSO2", 200.4f, 100});

                inputHandler.send(new Object[]{"IBM", 300.4f, 100});
                //Thread.sleep(100);
                inputHandler.send(new Object[]{"WSO2", 400.4f, 200});
                Thread.sleep(100);

                //persisting
                siddhiAppRuntime.persist();
                Thread.sleep(5000);

                inputHandler.send(new Object[]{"IBM", 100.4f, 100});
                //Thread.sleep(100);
                inputHandler.send(new Object[]{"WSO2", 200.4f, 100});

                inputHandler.send(new Object[]{"IBM", 300.4f, 100});
                //Thread.sleep(100);
                inputHandler.send(new Object[]{"WSO2", 400.4f, 200});
                Thread.sleep(100);

                //persisting
                siddhiAppRuntime.persist();
                Thread.sleep(5000);

                inputHandler.send(new Object[]{"IBM", 100.4f, 100});
                //Thread.sleep(100);
                inputHandler.send(new Object[]{"WSO2", 200.4f, 100});

                inputHandler.send(new Object[]{"IBM", 300.4f, 100});
                //Thread.sleep(100);
                inputHandler.send(new Object[]{"WSO2", 400.4f, 200});
                Thread.sleep(100);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @TearDown(Level.Trial)
        public void doTearDown() {
            try {
                System.out.println("Do TearDown");

                siddhiAppRuntime.shutdown();
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.SampleTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void testMethod(ApplicationState state) {
        state.siddhiAppRuntime.persist();
    }
}
