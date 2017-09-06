import client.RestClient;
import config.Config;
import config.Sqls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sql.SqlResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;

public class Benchmark {
    private static final Logger logger = LoggerFactory.getLogger(Benchmark.class);

    public static void main(String[] args) throws Exception {
                final String whereClause = Sqls.MSISDN_WhereClause;
//        final String whereClause = Sqls.IMSI_WhereClause;
        //        final String whereClause = Sqls.EVENT_BEGIN_DATE_WhereClause;

        Map<String, String[]> SQLWithAllCol = Sqls.getSQL(Sqls.selectAllColClause, whereClause);
        Map<String, String[]> SQLWithOneCol = Sqls.getSQL(Sqls.selectOneColClause, whereClause);
        Map<String, String[]> SQLWithTenCol = Sqls.getSQL(Sqls.selectTenColClause, whereClause);
        Map<String, String[]> SQLWithTwentyCol = Sqls.getSQL(Sqls.selectTwentyColClause, whereClause);
        warmUP(SQLWithAllCol, Config.project);
        testQueryLatency(SQLWithAllCol, "", Config.project, 1);
        testQueryLatency(SQLWithOneCol, "", Config.project, 1);
        testQueryLatency(SQLWithTenCol, "", Config.project, 1);
        testQueryLatency(SQLWithTwentyCol, "", Config.project, 1);

        //        testQueryLatency(SQLWithAllCol, Sqls.LIMIT_10000, Config.project, 1);
        //        testQueryLatency(SQLWithOneCol, Sqls.LIMIT_10000, Config.project, 1);
        //        testQueryLatency(SQLWithTenCol, Sqls.LIMIT_10000, Config.project, 1);
        //        testQueryLatency(SQLWithTwentyCol, Sqls.LIMIT_10000, Config.project, 1);
        /*
        Map<String, String[]> SQLWithAllCol = Sqls.getDataRangeSQL(Sqls.selectAllColClause, whereClause);
        Map<String, String[]> SQLWithOneCol = Sqls.getDataRangeSQL(Sqls.selectOneColClause, whereClause);
        Map<String, String[]> SQLWithTenCol = Sqls.getDataRangeSQL(Sqls.selectTenColClause, whereClause);
        Map<String, String[]> SQLWithTwentyCol = Sqls.getDataRangeSQL(Sqls.selectTwentyColClause, whereClause);
        warmUP(SQLWithAllCol, Config.project);
        testQueryLatency(SQLWithOneCol, Sqls.LIMIT_10000, Config.project, 1);
        testQueryLatency(SQLWithAllCol, Sqls.LIMIT_10000, Config.project, 1);
        testQueryLatency(SQLWithTenCol, Sqls.LIMIT_10000, Config.project, 1);
        testQueryLatency(SQLWithTwentyCol, Sqls.LIMIT_10000, Config.project, 1);
        */
    }

    public static void warmUP(Map<String, String[]> sqlMap, String project) throws IOException {
        logger.info("Start to warm KAP");
        RestClient client = new RestClient();
        client.disableCache();
        for (String key : sqlMap.keySet()) {
            for (String sql : sqlMap.get(key)) {
                client.query(sql + Sqls.LIMIT_9999, project);
            }
            logger.info("Warm " + key + " Succeed");
        }
    }

    public static void testQueryLatency(Map<String, String[]> sqlMap, String limit, String project, int times)
            throws Exception {
        System.out.println();
        logger.info("Start test single query's latency");
        RestClient client = new RestClient();
        client.disableCache();
        for (String key : sqlMap.keySet()) {
            List<SqlResponse> responses = new ArrayList<>();
            String[] sqls = sqlMap.get(key);
            for (String sql : sqls) {
                for (int i = 0; i < times; i++) {
                    SqlResponse sqlResponse = client.query(sql + limit, project);
                    System.out.println(sql + limit + " latency:" + sqlResponse.getSqlDuration());
                    responses.add(sqlResponse);
                }
            }
            double latency = avgQueryDuration(responses);
            double size = avgResultSize(responses);
            logger.info("---------------------------------------------------------------------------------------");
            logger.info(
                    "Test " + key + limit + " end, test {} quries, result size {}, the average query latency is:{} ms",
                    sqls.length, size, latency);
            logger.info("---------------------------------------------------------------------------------------");
        }
    }

    public static void testQueryThroughput(int threadNum, final String[] sqls, final String project)
            throws IOException, ExecutionException, InterruptedException {
        logger.info("Start to test query throughput, thread num is: {}", threadNum);
        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
        List<FutureTask<Double>> tasks = new ArrayList<>();
        final AtomicLong totalQueryCount = new AtomicLong(0);
        final AtomicLong totalQueryTime = new AtomicLong(0);

        for (int i = 0; i < threadNum; i++) {
            FutureTask<Double> task = new FutureTask<>(new Callable<Double>() {
                @Override
                public Double call() throws Exception {
                    long startTime = System.currentTimeMillis();
                    RestClient client = new RestClient();
                    client.disableCache();
                    List<SqlResponse> responses = new ArrayList<>();
                    while (true) {
                        for (String sql : sqls) {
                            SqlResponse resp = client.query(sql, project);
                            responses.add(resp);
                            totalQueryCount.incrementAndGet();
                            totalQueryTime.addAndGet(resp.getSqlDuration());
                            if (isTimeout(startTime, Config.TEST_DURATION)) {
                                return avgQueryDuration(responses);
                            }
                        }
                    }
                }
            });
            tasks.add(task);
            executorService.submit(task);
        }
        executorService.shutdown();
        double tps = 0.0;
        for (FutureTask<Double> task : tasks) {
            double singleRoundQueryLatency = task.get();
            logger.info("Query Throughput Test, one thread's query latency is: {}", singleRoundQueryLatency);
        }
        System.out.println(tps);
        logger.info("Test Query Throughput end, the QPS is: {}",
                (totalQueryCount.get() * 1.0 / (totalQueryTime.get() * 1.0 / 1000)) * threadNum);
    }

    private static double avgQueryDuration(List<SqlResponse> responses) {
        double total = 0.0;
        for (SqlResponse response : responses) {
            total += response.getSqlDuration();
        }
        return total / responses.size();
    }

    private static double avgResultSize(List<SqlResponse> responses) {
        double total = 0.0;
        for (SqlResponse response : responses) {
            total += response.getSize();
        }
        return total / responses.size();
    }

    private static boolean isTimeout(long startTime, long duration) {
        return (System.currentTimeMillis() - startTime) > duration;
    }
}
