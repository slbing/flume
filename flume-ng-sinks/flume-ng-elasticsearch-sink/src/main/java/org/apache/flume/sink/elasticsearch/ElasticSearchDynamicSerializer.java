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
package org.apache.flume.sink.elasticsearch;

import java.io.IOException;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.gson.JsonObject;

/**
 * Basic serializer that serializes the event body and header fields into
 * individual fields
 * </p>
 *
 * A best effort will be used to determine the content-type, if it cannot be
 * determined fields will be indexed as Strings
 */
public class ElasticSearchDynamicSerializer implements ElasticSearchEventSerializer {

	@Override
	public void configure(Context context) {
		// NO-OP...
	}

	@Override
	public void configure(ComponentConfiguration conf) {
		// NO-OP...
	}

	@Override
	public JSONObject getContent(Event event) throws IOException, JSONException {
		JSONObject ret = new JSONObject();

		appendBody(ret, event);
		appendHeaders(ret, event);
		return ret;
	}

	private void appendBody(JSONObject builder, Event event) throws IOException, JSONException {
		builder.put("body",new JSONObject(new String(event.getBody(),"utf-8")));
	}

	private void appendHeaders(JSONObject builder, Event event) throws IOException, JSONException {
		Map<String, String> headers = event.getHeaders();
		for (String key : headers.keySet()) {
			builder.put(key, headers.get(key));
		}
	}

}
