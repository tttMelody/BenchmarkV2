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
package io.kyligence.benchmark.loadtest.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiefan on 16-12-30.
 */
public class SqlStat {

    private static final Logger logger = LoggerFactory.getLogger(SqlStat.class);

    private SqlRequest sqlRequest;

    private List<SqlResponse> responses = new ArrayList<>();

    public SqlStat() {

    }

    public SqlRequest getSqlRequest() {
        return sqlRequest;
    }

    public void setSqlRequest(SqlRequest sqlRequest) {
        this.sqlRequest = sqlRequest;
    }

    public List<SqlResponse> getResponses() {
        return responses;
    }

    public void addResponse(SqlResponse response) {
        this.responses.add(response);
    }

    public void dump() {
        logger.info("---------------------------------------------");
        logger.info("sql file name : " + sqlRequest.getFileName());
        logger.info("is storageCacheUsed : " + isHitStorageCache());
        logger.info("total scan count : " + responses.get(0).getTotalScanCount());
        logger.info("avg query duration : " + avgQueryDuration() + " ms");
        logger.info("duration for each time : " + durationForEachTime());
    }

    public void dumpDetail() {

    }

    public boolean isHitStorageCache() {
        boolean flag = false;
        for (SqlResponse response : responses) {
            if (response.isHitStorageCache()) {
                flag = true;
            }
        }
        return flag;
    }

    public long avgQueryDuration() {
        long total = 0;
        for (SqlResponse response : responses) {
            total += response.getSqlDuration();
        }
        long avg = total / responses.size();
        return avg;
    }

    public List<Integer> durationForEachTime() {
        List<Integer> results = new ArrayList<>();
        for (SqlResponse response : responses) {
            results.add(response.getSqlDuration());
        }
        return results;
    }

}
