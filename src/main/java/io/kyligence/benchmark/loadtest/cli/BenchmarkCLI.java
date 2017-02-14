package io.kyligence.benchmark.loadtest.cli;

import io.kyligence.benchmark.loadtest.job.BenchmarkTestJob;
import io.kyligence.benchmark.loadtest.job.FullRegressionJob;
import io.kyligence.benchmark.loadtest.job.Job;
import io.kyligence.benchmark.loadtest.job.LoadTestJob;
import io.kyligence.benchmark.loadtest.job.TestCase;

/**
 * Created by xiefan on 17-2-13.
 */
public class BenchmarkCLI {
    public static void main(String[] args) throws Exception{
        String path;
        if(args.length != 1){
            throw new Exception("Error. Usage : java -jar BenchmarkCli.jar <test-case-config-file-path>");
        }
        else{
            path = args[0].trim();
            System.out.println("Using test case file : " + path);
        }
        TestCase testCase = new TestCase(path);
        FullRegressionJob job = new FullRegressionJob(testCase);
        //System.out.println("ok");
        job.run();
        job.dump();
    }
}
