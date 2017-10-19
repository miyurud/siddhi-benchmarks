
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
package org.wso2.siddhi.common.benchmarks.reorder;


import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;


/**
 * A data generator which uses data set.
 */
public class DataLoader extends Thread {
    private InputHandler inputHandler;
    private static final Logger log = Logger.getLogger(DataLoader.class);
    private boolean shutdownFlag = false;
    private String filePath = "/home/nishanthini/Project/Benchmark/My folder/siddhi-benchmarks/0001"
            + "/dataset1.csv";
    private Splitter splitter = Splitter.on(',');

    public DataLoader(InputHandler inputHandler) {
        super("Data Loader");
        this.inputHandler = inputHandler;
    }

    public void run() {

        BufferedReader bufferedReader = null;
        try {

            String line;
            FileInputStream fstream = new FileInputStream(filePath);
            bufferedReader = new BufferedReader(new InputStreamReader(fstream, Charsets.UTF_8));
            Object[] dataItem;
            float i = 0.0f;
            int j = 0;
            while ((line = bufferedReader.readLine()) != null) {


                Iterator<String> dataStrIterator = splitter.split(line).iterator();
                String time = dataStrIterator.next();
                String value = dataStrIterator.next();
                double data = Double.parseDouble(value);
                long timeStamp = Long.parseLong(time);
//


                try {
                    dataItem = new Object[]{timeStamp, i, data};
                    inputHandler.send(dataItem);


                } catch (InterruptedException e) {
                    log.error("Error sending an event to Input Handler, " + e.getMessage(), e);
                }
                i = i + 1.0f;
                j++;
            }
        } catch (FileNotFoundException e) {
            log.error("Error in accessing the input file. " + e.getMessage(), e);
        } catch (IOException e2) {
            log.error("Error. " + e2.getMessage(), e2);
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    log.error("Error in accessing the input file. " + e.getMessage(), e);
                }
            }
        }

    }

    public void shutdown() {
        shutdownFlag = true;
    }

}
