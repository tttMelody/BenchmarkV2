package io.kyligence.benchmark.loadtest.job;

import io.kyligence.benchmark.loadtest.config.ConfigBase;
import io.kyligence.benchmark.loadtest.sql.SqlRequest;
import io.kyligence.benchmark.loadtest.util.BenchmarkUtil;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Created by xiefan on 17-1-4.
 */
public class TestCase extends ConfigBase {

    public static final String DEFAULT_TEST_CASE_FILE_NAME = "testcase.properties";

    public static final String THREAD_LOCAL_QUERY_COUNT_KEY = "THREAD_LOCAL_QUERY_COUNT_KEY";

    public static final String THREEAD_LOCAL_QUERY_TIME_KEY = "THREEAD_LOCAL_QUERY_TIME_KEY";

    public static final String THREAD_ID_KEY = "THREAD_ID_KEY";

    private static final Logger logger = LoggerFactory.getLogger(TestCase.class);

    private List<SqlRequest> requests = new ArrayList<>();

    public TestCase(String configFile) throws IOException {
        super(configFile);
        init();
    }

    private void init() throws IOException {
        File queries = new File(getSqlFilesDir());
        if (!queries.exists()) {
            throw new IOException("Sql file or dir not exist");
        }
        if (queries.isDirectory()) { // dir
            File[] files = queries.listFiles();
            Arrays.sort(files, new Comparator<File>() {
                public int compare(File o1, File o2) {
                    return o1.getName().compareTo(o2.getName());
                }
            });
            for (File f : files) {
                SqlRequest request = new SqlRequest(FileUtils.readFileToString(f), f.getName());
                requests.add(request);
            }
        } else { // one file
            SqlRequest request = new SqlRequest(FileUtils.readFileToString(queries), queries.getName());
            requests.add(request);
        }
        logger.info("Successfull init test case");
        logger.info("Sql files Dir : {}", queries.getAbsolutePath());
        logger.info("Sql files num : {}", requests.size());
        logger.info("BuildCubeJob info. CubeName : {}. BuildStartTime : {}. BuildEndTime : {}", getCubeName(),
                getBuildStartTime(), getBuildEndTime());
    }

    public String getCubeName() {
        return properties.getProperty("build-cube-job.cubename");
    }

    public long getBuildStartTime() {
        String str = properties.getProperty("build-cube-job.build-start-time");
        return BenchmarkUtil.convert2long(str, BenchmarkUtil.DATE_FORMAT);
    }

    public long getBuildEndTime() {
        String str = properties.getProperty("build-cube-job.build-end-time");
        return BenchmarkUtil.convert2long(str, BenchmarkUtil.DATE_FORMAT);
    }

    public String getSqlFilesDir() {
        String str = properties.getProperty("sql-files-path");
        return str;
    }

    public List<SqlRequest> getAllSqlRequest() {
        return requests;
    }

    // RestClient

    public String getHost() {
        return getOptional("restclient.kylin-server-host", "localhost");
    }

    public int getPort() {
        int port = Integer.parseInt(getOptional("restclient.kylin-server-port", "7070"));
        return port;
    }

    public String getUserName() {
        return getOptional("restclient.kylin-server-username", "ADMIN");
    }

    public String getPassword() {
        return getOptional("restclient.kylin-server-password", "KYLIN");
    }

    public void setHost(String host) {
        properties.setProperty("restclient.kylin-server-host", host);
    }

    public void setPort(String port) {
        properties.setProperty("restclient.kylin-server-port", port);
    }

    // BuildCubeJob
    public long getSheckCubeStatusInterval() {
        String str = getOptional("build-cube-job.check-cube-status-interval", "60000");
        return Long.parseLong(str);
    }

    // BenchmarkTestJob
    public String getBenchmarkTestProjectName() {
        String str = getOptional("benchmark-project-name", "ssb_benchmark");
        return str;
    }

    public int getSqlLogNum() {
        String str = getOptional("sql-log-finish-num", "10");
        return Integer.parseInt(str);
    }

    public int getBenchmarkTestRepeatTime() {
        String str = getOptional("benchmark-test.repeat-time", "3");
        return Integer.parseInt(str);
    }

    // StressTestJob
    public String getLoadTestProjectName() {
        String str = getOptional("load-test-project-name", "ssb_stress");
        return str;
    }

    public int getLoadThreadNum() {
        String numStr = getOptional("load-test-job.thread-num", "3");
        return Integer.parseInt(numStr);
    }

    public long getLoadTestingTime() {
        String numStr = getOptional("load-test-job.testing-time", "10000");
        return Long.parseLong(numStr);
    }

    public long getLoadTestLogIntervalMs() {
        String numStr = getOptional("load-test-job.log-interval-ms", "1000");
        return Long.parseLong(numStr);
    }

    // run job config
    public boolean isNeedRebuildCube() {
        String str = getOptional("is-need-rebuild-cube", "false");
        return Boolean.parseBoolean(str);
    }

    public boolean isRunBenchmarkTest() {
        String str = getOptional("is-need-benchmark-test", "true");
        return Boolean.parseBoolean(str);
    }

    public boolean isRunStressTest() {
        String str = getOptional("is-need-load-test", "true");
        return Boolean.parseBoolean(str);
    }

    public String getHostUrl() {
        return properties.getProperty("host-url");
    }

    public String getPortUrl() {
        return properties.getProperty("port-url");
    }

    // multi instance config
    public String getScheduleMode() {
        return getOptional("multi-instance.schedule-mode", "concurrent");
    }

}
