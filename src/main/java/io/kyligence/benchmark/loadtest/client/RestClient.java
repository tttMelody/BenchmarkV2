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

package io.kyligence.benchmark.loadtest.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kyligence.benchmark.loadtest.job.TestCase;
import io.kyligence.benchmark.loadtest.sql.SqlResponse;
import io.kyligence.benchmark.loadtest.util.JsonUtil;

import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.util.EntityUtils;

import javax.xml.bind.DatatypeConverter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author yangli9
 */
public class RestClient {

    protected String host;

    protected int port;

    protected String baseUrl;

    protected String userName;

    protected String password;

    protected DefaultHttpClient client;

    private final int ONE_HOUR_IN_MS = 60 * 60 * 1000;

    public RestClient(TestCase testCase) {

        String host = testCase.getHost();
        int port = testCase.getPort();
        String userName = testCase.getUserName();
        String passwork = testCase.getPassword();

        init(host, port, userName, passwork);

    }

    private void init(String host, int port, String userName, String password) {

        this.host = host;
        this.port = port;
        this.userName = userName;
        this.password = password;
        this.baseUrl = "http://" + host + ":" + port + "/kylin/api";

        this.client = new DefaultHttpClient();

        if (userName != null && password != null) {
            CredentialsProvider provider = new BasicCredentialsProvider();
            UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(userName, password);
            provider.setCredentials(AuthScope.ANY, credentials);
            client.setCredentialsProvider(provider);
        }

        client.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, ONE_HOUR_IN_MS);
        client.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, ONE_HOUR_IN_MS);

    }

    private boolean setCache(boolean setValue) throws IOException {

        String url = baseUrl + "/admin/config";

        HttpPut put = newPut(url);

        HashMap<String, String> parameterMap = new HashMap<String, String>();
        parameterMap.put("key", "kylin.query.cache-enabled");
        parameterMap.put("value", setValue + "");
        put.setEntity(new StringEntity(new ObjectMapper().writeValueAsString(parameterMap), "UTF-8"));

        boolean setSuccessfully;

        try {

            HttpResponse response = client.execute(put);

            if (response.getStatusLine().getStatusCode() != 200) {
                setSuccessfully = false;
            } else {
                setSuccessfully = true;
            }

            EntityUtils.consume(response.getEntity());
            return setSuccessfully;

        } finally {
            put.releaseConnection();
        }

    }

    public void wipeCache(String entity, String event, String cacheKey) throws IOException {

        String url = baseUrl + "/cache/" + entity + "/" + cacheKey + "/" + event;

        HttpPut put = new HttpPut(url);

        try {

            HttpResponse response = client.execute(put);

            String msg = EntityUtils.toString(response.getEntity());
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new IOException("Invalid response " + response.getStatusLine().getStatusCode()
                        + " with cache wipe url " + url + "\n" + msg);
            }

            EntityUtils.consume(response.getEntity());

        } catch (Exception ex) {
            throw new IOException(ex);
        } finally {
            put.releaseConnection();
        }

    }

    public String getKylinProperties() throws IOException {

        String url = baseUrl + "/admin/config";

        HttpGet get = new HttpGet(url);

        try {

            HttpResponse response = client.execute(get);

            String msg = EntityUtils.toString(response.getEntity());
            Map<String, String> map = JsonUtil.readValueAsMap(msg);
            msg = map.get("config");

            if (response.getStatusLine().getStatusCode() != 200) {
                throw new IOException("Invalid response " + response.getStatusLine().getStatusCode()
                        + " with cache wipe url " + url + "\n" + msg);
            }

            EntityUtils.consume(response.getEntity());
            return msg;

        } finally {
            get.releaseConnection();
        }

    }

    public boolean enableCache() throws IOException {
        return setCache(true);
    }

    public boolean disableCache() throws IOException {
        return setCache(false);
    }

    public boolean buildCube(String cubeName, long startTime, long endTime, String buildType) throws Exception {

        String url = baseUrl + "/cubes/" + cubeName + "/build";

        HttpPut put = newPut(url);

        HashMap<String, String> parameterMap = new HashMap<String, String>();
        parameterMap.put("startTime", startTime + "");
        parameterMap.put("endTime", endTime + "");
        parameterMap.put("buildType", buildType);
        String jsonMsg = new ObjectMapper().writeValueAsString(parameterMap);
        put.setEntity(new StringEntity(jsonMsg, "UTF-8"));

        try {

            HttpResponse response = client.execute(put);
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new IOException("Invalid response " + response.getStatusLine().getStatusCode()
                        + " with build cube url " + url + "\n" + jsonMsg);
            }

            EntityUtils.consume(response.getEntity());
            return true;

        } finally {
            put.releaseConnection();
        }

    }

    public boolean disableCube(String cubeName) throws Exception {
        return changeCubeStatus(baseUrl + "/cubes/" + cubeName + "/disable");
    }

    public boolean enableCube(String cubeName) throws Exception {
        return changeCubeStatus(baseUrl + "/cubes/" + cubeName + "/enable");
    }

    public boolean purgeCube(String cubeName) throws Exception {
        return changeCubeStatus(baseUrl + "/cubes/" + cubeName + "/purge");
    }

    public HashMap<?, ?> getCube(String cubeName) throws Exception {

        String url = baseUrl + "/cubes/" + cubeName;

        HttpGet get = newGet(url);

        get.setURI(new URI(url));

        try {
            HttpResponse response = client.execute(get);
            return dealResponse(response);
        } finally {
            get.releaseConnection();
        }

    }

    private boolean changeCubeStatus(String url) throws Exception {

        HttpPut put = newPut(url);

        HashMap<String, String> parameterMap = new HashMap<String, String>();
        String jsonMsg = new ObjectMapper().writeValueAsString(parameterMap);
        put.setEntity(new StringEntity(jsonMsg, "UTF-8"));

        try {

            HttpResponse response = client.execute(put);

            if (response.getStatusLine().getStatusCode() != 200) {
                throw new IOException("Invalid response " + response.getStatusLine().getStatusCode() + " with url "
                        + url + "\n" + jsonMsg);
            } else {
                EntityUtils.consume(response.getEntity());
                return true;
            }
        } finally {
            put.releaseConnection();
        }

    }

    public SqlResponse query(String sql, String project) throws IOException {

        String url = baseUrl + "/query";

        HttpPost post = newPost(url);

        HashMap<String, String> parameterMap = new HashMap<String, String>();
        parameterMap.put("sql", sql);
        parameterMap.put("project", project);
        String jsonMsg = new ObjectMapper().writeValueAsString(parameterMap);
        post.setEntity(new StringEntity(jsonMsg, "UTF-8"));

        try {

            HttpResponse response = client.execute(post);
            SqlResponse r = new SqlResponse(dealResponse(response));
            EntityUtils.consume(response.getEntity());
            return r;

        } finally {
            post.releaseConnection();
        }
    }

    private HashMap<?, ?> dealResponse(HttpResponse response) throws IOException {

        String result = getContent(response);
        HashMap<?, ?> resultMap = new ObjectMapper().readValue(result, HashMap.class);
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Invalid response " + response.getStatusLine().getStatusCode() + "  Error message : "
                    + resultMap.get("exception"));
        }
        EntityUtils.consume(response.getEntity());

        return resultMap;

    }

    private void addHttpHeaders(HttpRequestBase method) {

        // method.addHeader("Accept", "application/json, text/plain, */*");
        method.addHeader("Accept", "application/vnd.apache.kylin-v2+json, text/plain, */*");
        method.addHeader("Content-Type", "application/json");
        String basicAuth = DatatypeConverter.printBase64Binary((this.userName + ":" + this.password).getBytes());
        method.addHeader("Authorization", "Basic " + basicAuth);

    }

    private HttpPost newPost(String url) {
        HttpPost post = new HttpPost(url);
        addHttpHeaders(post);
        return post;
    }

    private HttpPut newPut(String url) {
        HttpPut put = new HttpPut(url);
        addHttpHeaders(put);
        return put;
    }

    private HttpGet newGet(String url) {
        HttpGet get = new HttpGet();
        addHttpHeaders(get);
        return get;
    }

    private String getContent(HttpResponse response) throws IOException {
        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        StringBuffer result = new StringBuffer();
        String line = null;
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }
        return result.toString();
    }

}
