/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.internal.handlers.pulln;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.summary.ResultSummary;

public class AsyncPullResponseHandler implements PullResponseHandler
{
    private boolean doneStreaming = false;
    private final Queue<Record> records = new ArrayDeque<>();
    private CompletableFuture<Record> recordFuture;

    private final BasicPullResponseHandler delegate;
    private final PullController controller;

    private final CompletableFuture<ResultSummary> summaryFuture = new CompletableFuture<>();

    public AsyncPullResponseHandler( BasicPullResponseHandler handler )
    {
        this.delegate = handler;
        this.controller = new AutoPullController( delegate );

        delegate.installSummaryConsumer( ( summary, error ) -> {
            if ( summary != null )
            {
                summaryFuture.complete( summary );
            }
            else if ( error != null )
            {
                summaryFuture.completeExceptionally( error );
            }
            else
            {
                // has_more
                controller.handleSuccessWithHasMore();
            }
        } );

        delegate.installRecordConsumer( ( record, throwable ) -> {
            if ( record != null )
            {
                records.add( record );
                controller.afterEnqueueRecord( record.size() );
            }
            else if ( throwable != null )
            {
                failRecordFuture( throwable );
            }
            else
            {
                doneStreaming = true;
            }
        } );
    }

    @Override
    public synchronized Queue<Record> queue()
    {
        return records;
    }

    @Override
    public void request( long n )
    {
        delegate.request( n );
    }

    @Override
    public void cancel()
    {
        delegate.cancel();
    }

    public synchronized CompletableFuture<Record> nextRecord()
    {
        return peekRecord().thenApply( ignored -> {
            Record record = records.poll();
            controller.afterDequeueRecord( records.size(), delegate.status() == BasicPullResponseHandler.Status.Paused );
            return record;
        } );
    }

    public synchronized CompletableFuture<Record> peekRecord()
    {
        if ( recordFuture != null )
        {
            return recordFuture;
        }

        recordFuture = new CompletableFuture<>();
        if ( doneStreaming )
        {
            CompletableFuture<Record> ret = recordFuture;
            completeRecordFuture( null );
            return ret;
        }
        else
        {
            CompletableFuture<Record> ret = recordFuture;
            Record record = records.peek();
            if ( record != null )
            {
                completeRecordFuture( record );
            }
            return ret;
        }
    }

    public synchronized CompletionStage<ResultSummary> summary()
    {
        return summaryFuture;
    }

    private void failRecordFuture( Throwable error )
    {
        if ( recordFuture != null )
        {
            CompletableFuture<Record> future = recordFuture;
            recordFuture = null;
            future.completeExceptionally( error );
        }
    }

    private void completeRecordFuture( Record record )
    {
        if ( recordFuture != null )
        {
            CompletableFuture<Record> future = recordFuture;
            recordFuture = null;
            future.complete( record );
        }
    }
}
