package io.kyligence.benchmark.loadtest.it;

import io.kyligence.benchmark.loadtest.job.FullRegressionJob;
import io.kyligence.benchmark.loadtest.job.TestCase;
import org.junit.Test;

/**
 * Created by xiefan on 17-2-14.
 */
public class ITSSBBenchmarkTest {

    @Test
    public void fullRegressionJobTest() throws Exception {
        String configFilePath = "workload/conf/testcase.properties";
        TestCase testCase = new TestCase(configFilePath);
        FullRegressionJob job = new FullRegressionJob(testCase);
        job.run();
        job.dump();
    }
}
