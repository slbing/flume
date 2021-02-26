/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.elasticsearch.client;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.elasticsearch.IndexNameBuilder;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;

/**
 * Rest ElasticSearch client which is responsible for sending bulks of events to
 * ElasticSearch using ElasticSearch HTTP API. This is configurable, so any
 * config params required should be taken through this.
 */
public class ElasticSearchRestClient implements ElasticSearchClient {
  
  
  
  private static final Logger logger = LoggerFactory
      .getLogger(ElasticSearchRestClient.class);
  
  private final ElasticSearchEventSerializer serializer;
  
  BulkRequest bulkRequest = new BulkRequest();
  
  private RestHighLevelClient client = null;
  
  public ElasticSearchRestClient(String[] hostNames,
      ElasticSearchEventSerializer serializer) {
    
    HttpHost[] httpHosts = new HttpHost[hostNames.length];
    for (int i = 0; i < hostNames.length; i++) {
      httpHosts[i] = HttpHost.create(hostNames[i]);
    }
    RestClientBuilder builder = RestClient.builder(httpHosts);
    client = new RestHighLevelClient(builder);
    this.serializer = serializer;
    
  }
  
  @VisibleForTesting
  public ElasticSearchRestClient(String[] hostNames,
      ElasticSearchEventSerializer serializer, RestHighLevelClient client) {
    this(hostNames, serializer);
    this.client = client;
  }
  
  @Override
  public void configure(Context context) {}
  
  @Override
  public void close() {}
  
  @Override
  public void addEvent(Event event, IndexNameBuilder indexNameBuilder,
      String indexType, long ttlMs) throws Exception {
    try {
      JSONObject content = serializer.getContent(event);
      if (content == null) return;
      
      IndexRequest indexRequest = new IndexRequest(
          indexNameBuilder.getIndexName(event), indexType).source(content);
      
      synchronized (bulkRequest) {
        bulkRequest.add(indexRequest);
      }
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("skip this event:" + e.getMessage());
    }
  }
  
  public static class BulkActionListenerc
      implements ActionListener<BulkResponse> {
    
    @Override
    public void onResponse(BulkResponse resp) {
      for (BulkItemResponse bir : resp.getItems()) {
        if (bir.isFailed()) {
          logger.info("index failed with:" + bir.getFailureMessage());
        }
      }
    }
    
    @Override
    public void onFailure(Exception e) {
      logger.info(ExceptionUtils.getFullStackTrace(e));
    }
    
  }
  
  @Override
  public void execute() throws Exception {
    
    synchronized (bulkRequest) {
      client.bulkAsync(bulkRequest, RequestOptions.DEFAULT,
          new BulkActionListenerc());
      bulkRequest = new BulkRequest();
    }
  }
}
