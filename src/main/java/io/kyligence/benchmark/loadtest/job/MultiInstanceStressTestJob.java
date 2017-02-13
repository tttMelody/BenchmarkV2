package io.kyligence.benchmark.loadtest.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by xiefan on 17-1-5.
 */
@Deprecated
public class MultiInstanceStressTestJob implements Job {

    private TestCase testCase;

    private ExecutorService executorService = Executors.newCachedThreadPool();

    private static final Logger logger = LoggerFactory.getLogger(MultiInstanceStressTestJob.class);

    private String overrideConfigRootPath;

    private List<TestCase> testCases;

    private List<LoadTestJob> jobs = new ArrayList<>();

    public MultiInstanceStressTestJob(TestCase testCase, String overrideConfigRootPath) {
        this.testCase = testCase;
        this.overrideConfigRootPath = overrideConfigRootPath;
        if (!this.overrideConfigRootPath.endsWith("/"))
            this.overrideConfigRootPath += "/";
    }

    @Override
    public boolean run() throws Exception {
        testCases = getTestCases();
        dumpKylinInstanceInfo(testCases);
        if (testCase.getScheduleMode().equals("concurrent")) {
            logger.info("Use Concurrent mode to run multi instance");
            final CountDownLatch latch = new CountDownLatch(testCases.size());
            for (final TestCase t : testCases) {
                final LoadTestJob job = new LoadTestJob(t);
                jobs.add(job);
                executorService.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            job.run();
                            latch.countDown();
                        } catch (Exception e) {
                            logger.error("MultiInstanceTestJob runnint error.", e);
                        }
                    }
                });
            }
            latch.await();
        } else {
            logger.info("Use one-by-one mode to run multi instance");
            for (final TestCase t : testCases) {
                final LoadTestJob job = new LoadTestJob(t);
                jobs.add(job);
                job.run();
            }
        }
        return true;
    }

    private List<TestCase> getTestCases() throws IOException {
        String[] hosts = testCase.getHostUrl().split(",");
        List<TestCase> testCasesForEveryInstance = new ArrayList<>();
        if (hosts.length > 1) {
            logger.info("Runing tests on multi kylin instance");
        }
        String[] ports = testCase.getPortUrl().split(",");
        assert (hosts.length == ports.length);
        for (int i = 0; i < hosts.length; i++) {
            TestCase localTestCase = new TestCase(testCase.getConfigFilePath());
            //reset host and port
            localTestCase.setHost(hosts[i]);
            localTestCase.setPort(ports[i]);
            //detect override files
            File overrideFile = new File(overrideConfigRootPath + hosts[i] + "_" + ports[i] + ".override");
            if (overrideFile.exists()) {
                logger.info("Detect override file : {}", overrideFile.getAbsolutePath());
                Properties p = new Properties();
                p.load(new FileInputStream(overrideFile));
                localTestCase.putAll(p);
            }
            testCasesForEveryInstance.add(localTestCase);
        }
        return testCasesForEveryInstance;
    }

    private void dumpKylinInstanceInfo(List<TestCase> testCases) {
        logger.info("Kylin Instances info");
        for (TestCase t : testCases) {
            logger.info("host : {} , port : {}", t.getHost(), t.getPort());
        }

    }

    @Override
    public void dump() {
        logger.info("-- MultiInstanceTestJob Summary --");
        float totalTps = 0;
        for (int i = 0; i < testCases.size(); i++) {
            logger.info("instance : {} , host : {} , port : {}", i, testCases.get(i).getHost(), testCases.get(i).getPort());
            jobs.get(i).dump();
            totalTps += jobs.get(i).getTotalTps();
        }
        logger.info("-- MultiInstanceTestJob Global statistics --");
        logger.info("total tps for multi instance : {}", totalTps);
    }
}
