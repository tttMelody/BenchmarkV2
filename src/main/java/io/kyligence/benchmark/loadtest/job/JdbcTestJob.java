package io.kyligence.benchmark.loadtest.job;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.benchmark.loadtest.client.JdbcClient;
import io.kyligence.benchmark.loadtest.client.RestClient;
import io.kyligence.benchmark.loadtest.sql.SqlRequest;

public class JdbcTestJob implements Job {

    private ExecutorService executorService = Executors.newCachedThreadPool();

    private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    private final int threadNum;

    private final long testingTime;

    private final long logIntervalMs;

    private static final Logger logger = LoggerFactory.getLogger(LoadTestJob.class);

    private List<StressThreadLocalResult> results = new ArrayList<>();

    private TestCase testCase;

    private float totalTps = 0;

    public JdbcTestJob(TestCase testCase) {
        this.testCase = testCase;
        this.threadNum = testCase.getLoadThreadNum();
        this.testingTime = testCase.getLoadTestingTime();
        this.logIntervalMs = testCase.getLoadTestLogIntervalMs();
    }

    @Override
    public boolean run() throws Exception {

        logger.info("Start to run JdbcTestJob");
        List<FutureTask<StressThreadLocalResult>> tasks = new ArrayList<>();
        final AtomicLong totalQueryCount = new AtomicLong(0);
        final AtomicLong totalQueryTime = new AtomicLong(0);
        final AtomicLong totalQueryCountOneRound = new AtomicLong(0);

        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                double avgTime = logIntervalMs * 1.0;
                logger.info("System tps : " + getTps(totalQueryCountOneRound.getAndSet(0), avgTime));
            }
        }, 0, logIntervalMs, TimeUnit.MILLISECONDS);

        for (int i = 0; i < threadNum; i++) {

            final int id = i + 1;

            FutureTask<StressThreadLocalResult> task = new FutureTask<StressThreadLocalResult>(

                    new Callable<StressThreadLocalResult>() {
                        @Override
                        public StressThreadLocalResult call() throws Exception {

                            try {

                                long startTime = System.currentTimeMillis();
                                long threadLocalQueryCount = 0;
                                long threadLocalTimeCost = 0;

                                RestClient restClient = new RestClient(testCase);
                                restClient.disableCache();

                                JdbcClient jdbcClient = new JdbcClient(testCase);

                                while (true) {

                                    for (SqlRequest request : testCase.getAllSqlRequest()) {

                                        long queryStartTime = System.currentTimeMillis();
                                        ResultSet resultSet = jdbcClient.query(request.getSql());
                                        long queryEndTime = System.currentTimeMillis();

                                        if (!checkResultCorrect(resultSet)) {

                                            throw new Exception("Result not correct");

                                        } else {

                                            threadLocalQueryCount++;
                                            totalQueryCount.incrementAndGet();
                                            totalQueryCountOneRound.incrementAndGet();
                                            threadLocalTimeCost += (queryEndTime - queryStartTime);
                                            totalQueryTime.addAndGet(queryEndTime - queryStartTime);

                                        }

                                        if (threadLocalQueryCount % testCase.getSqlLogNum() == 0) {

                                            logger.info("finish query : " + request.getFileName());

                                        }

                                        if (isTimeout(startTime)) {
                                            StressThreadLocalResult result = new StressThreadLocalResult();
                                            result.put(TestCase.THREAD_LOCAL_QUERY_COUNT_KEY, threadLocalQueryCount);
                                            result.put(TestCase.THREAD_LOCAL_QUERY_TIME_KEY, threadLocalTimeCost);
                                            result.put(TestCase.THREAD_ID_KEY, id);
                                            jdbcClient.close();
                                            return result;
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                logger.error("error", e);
                                return null;
                            }
                        }
                    });
            tasks.add(task);
            executorService.submit(task);
        }

        for (FutureTask<StressThreadLocalResult> t : tasks) {
            StressThreadLocalResult r = t.get();
            if (r == null) {
                throw new Exception("Jdbc test thread can not return correct result");
            } else {
                results.add(r);
            }
        }

        logger.info("JdbcTestJob end");
        scheduledExecutorService.shutdown();

        return true;

    }

    @Override
    public void dump() {

        logger.info("-----------------JdbcTestJob dump--------------------");
        long totalQueryCount = 0;
        long totalDelayTime = 0;
        for (StressThreadLocalResult result : results) {
            Long localQueryCount = (Long) result.get(TestCase.THREAD_LOCAL_QUERY_COUNT_KEY);
            totalQueryCount += localQueryCount;
            Long localQueryTime = (Long) result.get(TestCase.THREAD_LOCAL_QUERY_TIME_KEY);
            totalDelayTime += localQueryTime;
            totalTps += getTps(localQueryCount, localQueryTime);
        }

        for (StressThreadLocalResult result : results) {
            result.dump();
        }
        logger.info("Global result for Jdbc");
        logger.info("global query count : " + totalQueryCount);
        logger.info("global delay time : " + totalDelayTime);
        logger.info("global tps : " + totalTps);
        logger.info("-------------------------------------");

    }

    class StressThreadLocalResult extends HashMap<String, Object> {

        private static final long serialVersionUID = 1L;

        public void dump() {
            logger.info("thread id : " + (Integer) get(TestCase.THREAD_ID_KEY));
            Long c = (Long) get(TestCase.THREAD_LOCAL_QUERY_COUNT_KEY);
            Long t = (Long) get(TestCase.THREAD_LOCAL_QUERY_TIME_KEY);
            logger.info("thread local query count : " + c);
            logger.info("thread local delay time : " + t);
            logger.info("thread local tps : " + getTps(c, t));
        }
    }

    private boolean isTimeout(long startTime) {
        if ((System.currentTimeMillis() - startTime) <= this.testingTime) {
            return false;
        }
        return true;
    }

    private double getTps(long totalCount, double totalTimeInMs) {
        double totalTimeSecond = totalTimeInMs * 1.0 / 1000;
        double tps = totalCount / totalTimeSecond;
        return tps;
    }

    private boolean checkResultCorrect(ResultSet resultSet) {
        try {
            return resultSet.next();
        } catch (SQLException e) {
            return false;
        }
    }

}
