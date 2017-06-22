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
package io.kyligence.benchmark.loadtest;

import io.kyligence.benchmark.loadtest.client.RestClient;
import io.kyligence.benchmark.loadtest.job.BenchmarkTestJob;
import io.kyligence.benchmark.loadtest.job.BuildCubeJob;
import io.kyligence.benchmark.loadtest.job.FullRegressionJob;
import io.kyligence.benchmark.loadtest.job.Job;
import io.kyligence.benchmark.loadtest.job.MultiInstanceStressTestJob;
import io.kyligence.benchmark.loadtest.job.LoadTestJob;
import io.kyligence.benchmark.loadtest.job.TestCase;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Ignore
public class SSBBenchmarkTest {

    private static final Logger logger = LoggerFactory.getLogger(SSBBenchmarkTest.class);

    private static String RESOURCE_ROOT = "src/test/resources/";

    public static final String TEST_CASE_FILE = RESOURCE_ROOT + TestCase.DEFAULT_TEST_CASE_FILE_NAME;

    private TestCase testCase;

    private RestClient client;

    @Before
    public void before() throws IOException {
        testCase = new TestCase(TEST_CASE_FILE);
        client = new RestClient(testCase);
        client.disableCache();
    }

    @Test
    public void testGetConfig() throws Exception {
        assertEquals("master", testCase.getHost());
        assertEquals(7170, testCase.getPort());
        assertEquals("ADMIN", testCase.getUserName());
        assertEquals("KYLIN", testCase.getPassword());
    }

    @Test
    public void testOpenSqlFiles() throws IOException {
        File f = new File(testCase.getSqlFilesDir());
        for (File file : f.listFiles()) {
            logger.info(file.getName());
        }
    }

    @Test
    public void testBasicBenchmark() throws Exception {

        Job job = new BenchmarkTestJob(testCase);
        job.run();
        job.dump();
    }

    @Test
    public void testStress() throws Exception {
        LoadTestJob job = new LoadTestJob(testCase);
        job.run();
        job.dump();
    }

}
