#!/bin/bash
# ---------------------------------------------------------------------------
#  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -----------------------------------------------------------------------------	
export PATH=$JAVA_HOME/bin:$PATH
CLASSPATH=.:target/classes:target/full-checkpointing-uberjar.jar
#FLIGHT_RECORDER_FLAGS="-XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:StartFlightRecording=delay=300s,duration=300s,name=Test,filename=recording-restore-300s-jan28-full.jfr"
JAVA_OPTS=$FLIGHT_RECORDER_FLAGS" -Xmx8g -Xms8g -Dlog4j.configuration=file:///home/miyurud/Desktop/git/siddhi-benchmarks/incremental-checkpointing-4.0.9-SNAPSHOT/log4j.properties"
FULL_EXPERIMENT_DURATION_MINUTES=40
WARM_UP_PERIOD_MINS=10

java $JAVA_OPTS -cp $CLASSPATH  org.wso2.siddhi.common.benchmarks.persistance.LengthWindowIncrementalCheckpointingBenchmarkFullRestore $FULL_EXPERIMENT_DURATION_MINUTES $WARM_UP_PERIOD_MINS
