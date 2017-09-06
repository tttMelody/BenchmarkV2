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
package sql;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by xiefan on 16-12-30.
 */
public class SqlResponse {

    private int sqlDuration;

    private int totalScanCount;

    private int size;

    private boolean isHitStorageCache;

    public SqlResponse(HashMap rawResponse) {
        HashMap dataMap = (HashMap) rawResponse.get("data");
        this.sqlDuration = (Integer) dataMap.get("duration");
        this.totalScanCount = (Integer) dataMap.get("totalScanCount");
        this.isHitStorageCache = (Boolean) dataMap.get("storageCacheUsed");
        this.size = ((ArrayList) dataMap.get("results")).size();

    }

    public int getSqlDuration() {
        return sqlDuration;
    }

    public int getTotalScanCount() {
        return totalScanCount;
    }

    public boolean isHitStorageCache() {

        return isHitStorageCache;
    }

    public int getSize() {
        return size;
    }
}
