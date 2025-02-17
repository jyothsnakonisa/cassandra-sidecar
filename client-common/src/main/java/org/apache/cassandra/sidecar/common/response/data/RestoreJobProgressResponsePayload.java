/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.common.response.data;


import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.data.ConsistencyVerificationResult;
import org.apache.cassandra.sidecar.common.request.RestoreJobProgressRequest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_PROGRESS_ABORTED_RANGES;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_PROGRESS_FAILED_RANGES;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_PROGRESS_MESSAGE;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_PROGRESS_PENDING_RANGES;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_PROGRESS_STATUS;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_PROGRESS_SUCCEEDED_RANGES;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_PROGRESS_SUMMARY;

/**
 * A class representing a response for the {@link RestoreJobProgressRequest}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RestoreJobProgressResponsePayload
{
    private final String message;
    private final ConsistencyVerificationResult status;
    private final RestoreJobSummaryResponsePayload summary;
    // the ranges can be null/absent, if they are not required to be included in the response payload, depending on the fetch policy
    @Nullable // failed due to unrecoverable exceptions
    private final List<RestoreRangeJson> failedRanges;
    @Nullable // aborted due to the job has failed/aborted
    private final List<RestoreRangeJson> abortedRanges;
    @Nullable
    private final List<RestoreRangeJson> pendingRanges;
    @Nullable
    private final List<RestoreRangeJson> succeededRanges;

    /**
     * @return builder to build the {@link RestoreJobProgressResponsePayload}
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Constructor for json deserialization
     */
    @JsonCreator
    public RestoreJobProgressResponsePayload(@NotNull @JsonProperty(JOB_PROGRESS_MESSAGE) String message,
                                             @NotNull @JsonProperty(JOB_PROGRESS_STATUS) ConsistencyVerificationResult status,
                                             @NotNull @JsonProperty(JOB_PROGRESS_SUMMARY) RestoreJobSummaryResponsePayload summary,
                                             @Nullable @JsonProperty(JOB_PROGRESS_FAILED_RANGES) List<RestoreRangeJson> failedRanges,
                                             @Nullable @JsonProperty(JOB_PROGRESS_ABORTED_RANGES) List<RestoreRangeJson> abortedRanges,
                                             @Nullable @JsonProperty(JOB_PROGRESS_PENDING_RANGES) List<RestoreRangeJson> pendingRanges,
                                             @Nullable @JsonProperty(JOB_PROGRESS_SUCCEEDED_RANGES) List<RestoreRangeJson> succeededRanges)
    {
        this.message = message;
        this.status = status;
        this.summary = summary;
        this.failedRanges = failedRanges;
        this.abortedRanges = abortedRanges;
        this.pendingRanges = pendingRanges;
        this.succeededRanges = succeededRanges;
    }

    private RestoreJobProgressResponsePayload(Builder builder)
    {
        this(builder.message,
             builder.status,
             builder.summary,
             builder.failedRanges,
             builder.abortedRanges,
             builder.pendingRanges,
             builder.succeededRanges);
    }

    @NotNull
    @JsonProperty(JOB_PROGRESS_MESSAGE)
    public String message()
    {
        return this.message;
    }

    @NotNull
    @JsonProperty(JOB_PROGRESS_STATUS)
    public ConsistencyVerificationResult status()
    {
        return this.status;
    }

    @NotNull
    @JsonProperty(JOB_PROGRESS_SUMMARY)
    public RestoreJobSummaryResponsePayload summary()
    {
        return this.summary;
    }

    @Nullable
    @JsonProperty(JOB_PROGRESS_FAILED_RANGES)
    public List<RestoreRangeJson> failedRanges()
    {
        return this.failedRanges;
    }

    @Nullable
    @JsonProperty(JOB_PROGRESS_ABORTED_RANGES)
    public List<RestoreRangeJson> abortedRanges()
    {
        return this.abortedRanges;
    }

    @Nullable
    @JsonProperty(JOB_PROGRESS_PENDING_RANGES)
    public List<RestoreRangeJson> pendingRanges()
    {
        return this.pendingRanges;
    }

    @Nullable
    @JsonProperty(JOB_PROGRESS_SUCCEEDED_RANGES)
    public List<RestoreRangeJson> succeededRanges()
    {
        return this.succeededRanges;
    }

    /**
     * Builds {@link RestoreJobProgressResponsePayload}
     */
    public static class Builder implements DataObjectBuilder<Builder, RestoreJobProgressResponsePayload>
    {
        String message;
        ConsistencyVerificationResult status;
        RestoreJobSummaryResponsePayload summary;
        List<RestoreRangeJson> failedRanges = null;
        List<RestoreRangeJson> abortedRanges = null;
        List<RestoreRangeJson> pendingRanges = null;
        List<RestoreRangeJson> succeededRanges = null;

        public Builder withStatus(ConsistencyVerificationResult status)
        {
            return update(b -> b.status = status);
        }

        public Builder withMessage(String message)
        {
            return update(b -> b.message = message);
        }

        public Builder withJobSummary(String createdAt,
                                      UUID jobId,
                                      String jobAgent,
                                      String keyspace,
                                      String table,
                                      String status)
        {
            return update(b -> b.summary = new RestoreJobSummaryResponsePayload(createdAt, jobId, jobAgent, keyspace, table, null, status));
        }

        public Builder withFailedRanges(List<RestoreRangeJson> failedRanges)
        {
            return update(b -> b.failedRanges = copyNullableList(failedRanges));
        }

        public Builder withAbortedRanges(List<RestoreRangeJson> abortedRanges)
        {
            return update(b -> b.abortedRanges = copyNullableList(abortedRanges));
        }

        public Builder withPendingRanges(List<RestoreRangeJson> pendingRanges)
        {
            return update(b -> b.pendingRanges = copyNullableList(pendingRanges));
        }

        public Builder withSucceededRanges(List<RestoreRangeJson> succeededRanges)
        {
            return update(b -> b.succeededRanges = copyNullableList(succeededRanges));
        }

        private static List<RestoreRangeJson> copyNullableList(List<RestoreRangeJson> list)
        {
            return list == null ? null : new ArrayList<>(list);
        }

        @Override
        public Builder self()
        {
            return this;
        }

        @Override
        public RestoreJobProgressResponsePayload build()
        {
            return new RestoreJobProgressResponsePayload(self());
        }
    }
}
