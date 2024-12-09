/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.sidecar.common.request;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.netty.handler.codec.http.HttpMethod;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.utils.HttpRange;

/**
 * Represents a request to stream CDC segments(commit logs) on an instance.
 */
public class StreamCdcSegmentRequest extends Request
{
    private final String segment;
    private final HttpRange range;
    private final String batchId;
    private final String partitionId;

    public StreamCdcSegmentRequest(String segment, HttpRange range, String batchId, String partitionId)
    {
        super(requestURI(segment));
        this.segment = segment;
        this.range = range;
        this.batchId = batchId;
        this.partitionId = partitionId;
    }

    @Override
    public HttpMethod method()
    {
        return HttpMethod.GET;
    }

    @Override
    public Map<String, String> headers()
    {
        Map<String, String> headers = new HashMap<>(super.headers());
        headers.put("X-Stream-Batch", batchId);
        headers.put("X-Stream-Partition", partitionId);
        if (range != null)
        {
            headers.put("Range", String.format("bytes=%d-%d", range.start(), range.end()));
        }
        return Collections.unmodifiableMap(headers);
    }

    private static String requestURI(String segment)
    {
        return ApiEndpointsV1.STREAM_CDC_SEGMENTS_ROUTE.replaceAll(ApiEndpointsV1.SEGMENT_PATH_PARAM, segment);
    }
}
