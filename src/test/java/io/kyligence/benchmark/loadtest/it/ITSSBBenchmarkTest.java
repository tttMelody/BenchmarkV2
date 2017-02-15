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
        if (System.getProperty("conf.dir.path") == null) {
            throw new RuntimeException("No config dir set.");
        }
        String configFilePath = System.getProperty("conf.dir.path") + "/conf/testcase.properties";
        TestCase testCase = new TestCase(configFilePath);
        FullRegressionJob job = new FullRegressionJob(testCase);
        job.run();
        job.dump();
    }
}
