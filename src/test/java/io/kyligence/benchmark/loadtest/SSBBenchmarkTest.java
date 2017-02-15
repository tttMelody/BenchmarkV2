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

    private static final String INSTANCE_OVERRIDE_DIR = RESOURCE_ROOT + "instance-override";

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

    //@Ignore
    //@Test
    public void testBuildCube() throws Exception {
        boolean success = client.buildCube("ssb", testCase.getBuildStartTime(), testCase.getBuildEndTime(), "BUILD");
        assertTrue(success);
    }

    @Test
    public void testDisableCube() throws Exception {
        boolean success = client.disableCube(testCase.getCubeName());
        assertTrue(success);
    }

    @Test
    public void testEnableCube() throws Exception {
        boolean success = client.enableCube(testCase.getCubeName());
        assertTrue(success);
    }


    //dangerous
    //@Ignore
    @Test
    public void testPurgeCube() throws Exception {
        boolean success = client.purgeCube(testCase.getCubeName());
        assertTrue(success);
    }

    @Test
    public void testGetCube() throws Exception {
        HashMap result = client.getCube(testCase.getCubeName());
        logger.info((String) result.get("status"));
    }

    //@Ignore
    @Test
    public void testBuildCubeJob() throws Exception {
        Job job = new BuildCubeJob(testCase);
        job.run();
        job.dump();
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

    @Test
    public void fullRegressionJobTest() throws Exception {
        FullRegressionJob job = new FullRegressionJob(testCase);
        job.run();
        job.dump();
    }

    @Test
    public void multiInstanceJobTest() throws Exception {
        MultiInstanceStressTestJob job = new MultiInstanceStressTestJob(testCase,RESOURCE_ROOT + "instance-override");
        job.run();
        job.dump();
    }

}
