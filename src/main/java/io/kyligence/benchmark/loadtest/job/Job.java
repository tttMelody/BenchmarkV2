package io.kyligence.benchmark.loadtest.job;

/**
 * Created by xiefan on 17-1-4.
 */
public interface Job {

    boolean run() throws Exception;

    void dump();
}
