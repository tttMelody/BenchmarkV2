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

    private int ONE_HOUR_IN_MS = 60 * 60 * 1000;

    public RestClient(TestCase testCase) {
        String user = testCase.getUserName();
        String pwd = testCase.getPassword();
        String host = testCase.getHost();
        int port = testCase.getPort();
        init(host, port, user, pwd);
    }

    private void init(String host, int port, String userName, String password) {
        this.host = host;
        this.port = port;
        this.userName = userName;
        this.password = password;
        this.baseUrl = "http://" + host + ":" + port + "/kylin/api";

        client = new DefaultHttpClient();

        if (userName != null && password != null) {
            CredentialsProvider provider = new BasicCredentialsProvider();
            UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(userName, password);
            provider.setCredentials(AuthScope.ANY, credentials);
            client.setCredentialsProvider(provider);
        }
        client.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, ONE_HOUR_IN_MS);
        client.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, ONE_HOUR_IN_MS);
    }

    public void wipeCache(String entity, String event, String cacheKey) throws IOException {
        String url = baseUrl + "/cache/" + entity + "/" + cacheKey + "/" + event;
        HttpPut request = new HttpPut(url);

        try {
            HttpResponse response = client.execute(request);
            String msg = EntityUtils.toString(response.getEntity());

            if (response.getStatusLine().getStatusCode() != 200)
                throw new IOException("Invalid response " + response.getStatusLine().getStatusCode()
                        + " with cache wipe url " + url + "\n" + msg);
        } catch (Exception ex) {
            throw new IOException(ex);
        } finally {
            request.releaseConnection();
        }
    }

    public String getKylinProperties() throws IOException {
        String url = baseUrl + "/admin/config";
        HttpGet request = new HttpGet(url);
        try {
            HttpResponse response = client.execute(request);
            String msg = EntityUtils.toString(response.getEntity());
            Map<String, String> map = JsonUtil.readValueAsMap(msg);
            msg = map.get("config");

            if (response.getStatusLine().getStatusCode() != 200)
                throw new IOException("Invalid response " + response.getStatusLine().getStatusCode()
                        + " with cache wipe url " + url + "\n" + msg);
            return msg;
        } finally {
            request.releaseConnection();
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
        HashMap<String, String> paraMap = new HashMap<String, String>();
        paraMap.put("startTime", startTime + "");
        paraMap.put("endTime", endTime + "");
        paraMap.put("buildType", buildType);
        String jsonMsg = new ObjectMapper().writeValueAsString(paraMap);
        put.setEntity(new StringEntity(jsonMsg, "UTF-8"));
        HttpResponse response = client.execute(put);
        String result = getContent(response);
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Invalid response " + response.getStatusLine().getStatusCode()
                    + " with build cube url " + url + "\n" + jsonMsg);
        } else {
            return true;
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

    public HashMap getCube(String cubeName) throws Exception {
        String url = baseUrl + "/cubes/" + cubeName;
        HttpGet get = newGet(url);
        get.setURI(new URI(url));
        HttpResponse response = client.execute(get);
        return dealResponse(response);
    }

    private boolean changeCubeStatus(String url) throws Exception {
        HttpPut put = newPut(url);
        HashMap<String, String> paraMap = new HashMap<String, String>();
        String jsonMsg = new ObjectMapper().writeValueAsString(paraMap);
        put.setEntity(new StringEntity(jsonMsg, "UTF-8"));
        HttpResponse response = client.execute(put);
        String result = getContent(response);
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Invalid response " + response.getStatusLine().getStatusCode() + " with url " + url
                    + "\n" + jsonMsg);
        } else {
            return true;
        }
    }

    public SqlResponse query(String sql, String project) throws IOException {
        String url = baseUrl + "/query";
        HttpPost post = newPost(url);
        HashMap<String, String> paraMap = new HashMap<String, String>();
        paraMap.put("sql", sql);
        paraMap.put("project", project);
        String jsonMsg = new ObjectMapper().writeValueAsString(paraMap);
        post.setEntity(new StringEntity(jsonMsg, "UTF-8"));
        HttpResponse response = client.execute(post);
        SqlResponse r = new SqlResponse(dealResponse(response));
        return r;
    }

    private HashMap dealResponse(HttpResponse response) throws IOException {
        String result = getContent(response);
        HashMap resultMap = new ObjectMapper().readValue(result, HashMap.class);
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Invalid response " + response.getStatusLine().getStatusCode() + "  Error message : "
                    + resultMap.get("exception"));
        }

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

    private boolean setCache(boolean flag) throws IOException {
        String url = baseUrl + "/admin/config";
        HttpPut put = newPut(url);
        HashMap<String, String> paraMap = new HashMap<String, String>();
        paraMap.put("key", "kylin.query.cache-enabled");
        paraMap.put("value", flag + "");
        put.setEntity(new StringEntity(new ObjectMapper().writeValueAsString(paraMap), "UTF-8"));
        HttpResponse response = client.execute(put);
        EntityUtils.consume(response.getEntity());
        if (response.getStatusLine().getStatusCode() != 200) {
            return false;
        } else {
            return true;
        }
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
