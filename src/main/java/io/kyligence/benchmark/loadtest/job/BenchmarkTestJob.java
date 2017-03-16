package io.kyligence.benchmark.loadtest.job;


import io.kyligence.benchmark.loadtest.client.RestClient;
import io.kyligence.benchmark.loadtest.sql.SqlRequest;
import io.kyligence.benchmark.loadtest.sql.SqlResponse;
import io.kyligence.benchmark.loadtest.sql.SqlStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiefan on 17-1-4.
 */
public class BenchmarkTestJob implements Job {

    private final TestCase testCase;

    private List<SqlStat> results = new ArrayList<>();

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkTestJob.class);

    public BenchmarkTestJob(TestCase testCase) {
        this.testCase = testCase;
    }

    public boolean run() throws Exception {
        logger.info("Start to run BenchmarkTestJob");
        RestClient client = new RestClient(testCase);
        client.disableCache();
        for (SqlRequest request : testCase.getAllSqlRequest()) {
            logger.info("execute sql file : " + request.getFileName());
            SqlStat stat = new SqlStat();
            stat.setSqlRequest(request);
            for (int i = 0; i < testCase.getBenchmarkTestRepeatTime(); i++) {
                SqlResponse response = client.query(request.getSql(), testCase.getBenchmarkTestProjectName());
                stat.addResponse(response);
            }

            results.add(stat);
        }
        logger.info("BenchmarkTestJob end");
        return true;
    }

    @Override
    public void dump() {
        logger.info("DBenchmarkTestJob dump");
        for (SqlStat stat : results) {
            stat.dump();
        }
    }
}
