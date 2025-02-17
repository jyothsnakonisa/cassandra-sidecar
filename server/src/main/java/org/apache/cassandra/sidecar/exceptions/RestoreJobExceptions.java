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

package org.apache.cassandra.sidecar.exceptions;

import org.apache.cassandra.sidecar.db.RestoreRange;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;

/**
 * Utility methods to create {@link RestoreJobException}
 */
public class RestoreJobExceptions
{
    private RestoreJobExceptions() {}

    /**
     * Create a {@link RestoreJobException} with cause.
     * If the cause is already a {@link RestoreJobException}, the retryable property is preserved.
     * @param cause
     * @return a new {@link RestoreJobException}
     */
    public static RestoreJobException propagate(Throwable cause)
    {
        return propagate(null, cause);
    }

    /**
     * Create a {@link RestoreJobException} with message and cause.
     * If the cause is already a {@link RestoreJobException}, the retryable property is preserved.
     * @param message
     * @param cause
     * @return a new {@link RestoreJobException}
     */
    public static RestoreJobException propagate(@Nullable String message, Throwable cause)
    {
        String concatMessage = message;
        if (cause.getMessage() != null)
        {
            concatMessage = concatMessage == null
                            ? cause.getMessage()
                            : concatMessage + ':' + cause.getMessage();
        }
        if (cause instanceof RestoreJobException)
        {
            RestoreJobException ex = (RestoreJobException) cause;
            return ex.retryable()
                   ? new RestoreJobException(concatMessage, cause)
                   : new RestoreJobFatalException(concatMessage, cause);
        }

        return new RestoreJobException(concatMessage, cause);
    }

    public static RestoreJobFatalException toFatal(Throwable cause)
    {
        if (cause instanceof RestoreJobFatalException)
            return (RestoreJobFatalException) cause;

        return new RestoreJobFatalException(cause.getMessage(), cause);
    }

    public static RestoreJobException of(String title, RestoreRange range, Throwable cause)
    {
        return new RestoreJobException(title + ". " + range.shortDescription(), cause);
    }

    public static RestoreJobException of(String title, RestoreRange range, AwsErrorDetails awsErrorDetails, Throwable cause)
    {
        return new RestoreJobException(title + ". " + range.shortDescription() + toString(awsErrorDetails), cause);
    }

    public static RestoreJobFatalException ofFatal(String title, RestoreRange range, Throwable cause)
    {
        return new RestoreJobFatalException(title + ". " + range.shortDescription(), cause);
    }

    public static RestoreJobFatalException ofFatal(String title, RestoreRange range, AwsErrorDetails awsErrorDetails, Throwable cause)
    {
        return new RestoreJobFatalException(title + ". " + range.shortDescription() + toString(awsErrorDetails), cause);
    }

    private static String toString(Object obj)
    {
        return obj == null ? "" : " " + obj;
    }
}
