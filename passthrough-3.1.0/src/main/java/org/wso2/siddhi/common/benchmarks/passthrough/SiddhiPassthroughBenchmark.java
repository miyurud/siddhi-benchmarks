/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.common.benchmarks.passthrough;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Passthrough benchmark for Siddhi.
 */
public class SiddhiPassthroughBenchmark {
    private static final Logger log = Logger.getLogger(SiddhiPassthroughBenchmark.class);
    private static long firstTupleTime = -1;
    private static String logDir = "./results-passthrough-3.1.0";
    private static final int RECORD_WINDOW = 10000; //This is the number of events to record.
    private static FileWriter fw = null;
    private static long eventCountTotal = 0;
    private static long eventCount = 0;
    private static long timeSpent = 0;
    private static long totalTimeSpent = 0;
    private static long startTime = System.currentTimeMillis();
    private static boolean flag;
    private static long veryFirstTime = System.currentTimeMillis();
    private static Writer fstream = null;

    public static void main(String[] args) {
        try {
            File directory = new File(logDir);

            if (!directory.exists()) {
                if (!directory.mkdir()) {
                    log.error("Error while creating the output directory.");
                }
            }

            fstream = new OutputStreamWriter(new FileOutputStream(new File(logDir + "/output-" +
                                                                                   getLogFileSequenceNumber() + "-" +

                                                                                   (System.currentTimeMillis())
                                                                                   + ".csv")
                                                                          .getAbsoluteFile()), StandardCharsets
                                                     .UTF_8);

        } catch (IOException e) {
            log.error("Error while creating statistics output file, " + e.getMessage(), e);
        }

        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream inputStream ( iij_timestamp long, value float);";
        String outputStream = "define stream outputStream ( iij_timestamp long, value float);";
        String query = "@info(name = 'query1') from inputStream " +
                "select iij_timestamp, value insert into outputStream;\";";

        ExecutionPlanRuntime executionPlanRuntime =
                siddhiManager.createExecutionPlanRuntime(inputStream + outputStream + query);
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        DataGenerator dataLoader = new DataGenerator(inputHandler);
        dataLoader.start();

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                for (Event evt : events) {
                    long currentTime = System.currentTimeMillis();

                    if (firstTupleTime == -1) {
                        firstTupleTime = currentTime;
                    }

                    long iijTimestamp = Long.parseLong(evt.getData()[0].toString());

                    try {
                        eventCount++;
                        eventCountTotal++;
                        timeSpent += (currentTime - iijTimestamp);

                        if (eventCount % RECORD_WINDOW == 0) {
                            totalTimeSpent += timeSpent;
                            long value = currentTime - startTime;

                            if (value == 0) {
                                value++;
                            }

                            if (!flag) {
                                flag = true;
                                fstream.write("Id, Throughput in this window (events/second), Entire throughput " +
                                                      "for the run (events/second), Total elapsed time(s), Average "
                                                      + "latency "
                                                      +
                                                      "per event (ms), Entire Average latency per event (ms), Total "
                                                      + "number"
                                                      + " of "
                                                      +
                                                      "events received (non-atomic)");
                                fstream.write("\r\n");
                            }

                            System.out.print(".");
                            fstream.write(
                                    (eventCountTotal / RECORD_WINDOW) + "," + ((eventCount * 1000) / value) + "," +
                                            ((eventCountTotal * 1000) / (currentTime - veryFirstTime)) + "," +
                                            ((currentTime - veryFirstTime) / 1000f) + "," + (timeSpent * 1.0
                                            / eventCount) +
                                            "," + ((totalTimeSpent * 1.0) / eventCountTotal) + "," + eventCountTotal);
                            fstream.write("\r\n");
                            fstream.flush();
                            startTime = System.currentTimeMillis();
                            eventCount = 0;
                            timeSpent = 0;
                        }
                    } catch (Exception e) {
                        log.error("Error while consuming event on incrementStream2, " + e.getMessage(), e);
                    }
                }
            }
        });


        while (true) {
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                log.error("Thread interrupted. " + e.getMessage(), e);
            }
        }
    }

    /**
     * This method returns a unique integer that can be used as a sequence number for log files.
     */
    private static int getLogFileSequenceNumber() {
        int result = -1;
        BufferedReader br = null;

        //read the flag
        try {
            String sCurrentLine;

            File directory = new File(logDir);

            if (!directory.exists()) {
                if (!directory.mkdir()) {
                    log.error("Error while creating the output directory.");
                }
            }

            File sequenceFile = new File(logDir + "/sequence-number.txt");

            if (sequenceFile.exists()) {
                br = new BufferedReader(new InputStreamReader(new FileInputStream(logDir + "/sequence-number.txt"),
                                                              Charset.forName("UTF-8")));

                while ((sCurrentLine = br.readLine()) != null) {
                    result = Integer.parseInt(sCurrentLine);
                    break;
                }
            }
        } catch (IOException e) {
            log.error("Error when reading the sequence number from sequence-number.txt file. " + e.getMessage(), e);
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        //write the new flag
        try {
            if (result == -1) {
                result = 0;
            }

            String content = "" + (result + 1); //need to increment by one for next round
            File file = new File(logDir + "/sequence-number.txt");

            // if file doesn't exists, then create it
            if (!file.exists()) {
                boolean fileCreateResults = file.createNewFile();
                if (!fileCreateResults) {
                    log.error("Error when creating the sequence-number.txt file.");
                }
            }

            Writer fstream = new OutputStreamWriter(new FileOutputStream(file.getAbsoluteFile()), StandardCharsets
                    .UTF_8);

            fstream.write(content);
            fstream.flush();
            fstream.close();
        } catch (IOException e) {
            log.error("Error when writing performance information. " + e.getMessage(), e);
        }

        return result;
    }
}
