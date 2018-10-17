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
package org.neo4j.driver.internal.handlers;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.messaging.request.PullNMessage;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.internal.util.MetadataExtractor;
import org.neo4j.driver.internal.value.BooleanValue;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.util.Function;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;

public abstract class PullNRequestAndResponseHandler implements PullHandler
{
    private static final Queue<Record> UNINITIALIZED_RECORDS = Iterables.emptyQueue();

    private final Statement statement;
    private final RunResponseHandler runResponseHandler;
    protected final MetadataExtractor metadataExtractor;
    protected final Connection connection;

    // initialized lazily when first record arrives
    private Queue<Record> records = UNINITIALIZED_RECORDS;

    private Throwable failure;
    private ResultSummary summary;

    private CompletableFuture<Record> recordFuture;
    private CompletableFuture<Throwable> failureFuture;
    private Status status;
    private volatile boolean isCancelled = false;

    // we automatically pull this amount of records when accessing a record that has not been pulled
    // This value cannot be less than 1
    private static final int AUTO_PULL_SIZE = 10;

    enum Status
    {
        Streaming
                {
                    void onRecord( PullNRequestAndResponseHandler handler, Value[] fields )
                    {
                        // we only check if it is cancelled here as we do not want both user thread and netty io thread to touch this status
                        if( handler.isCancelled )
                        {
                            handler.status = Cancelled;
                            handler.status.onRecord( handler, fields );
                            return;
                        }

                        // save records
                        Record record = new InternalRecord( handler.runResponseHandler.statementKeys(), fields );
                        handler.enqueueRecord( record );
                        handler.completeRecordFuture( record );

                    }
                },
        Finished
                {
                    void onSuccess( PullNRequestAndResponseHandler handler, Map<String,Value> metadata )
                    {
                        handler.extractResultSummary( metadata );
                        handler.afterSuccess( metadata );
                        handler.completeRecordFuture( null );
                        handler.completeFailureFuture( null );
                    }
                },
        HasMore
                {
                    void onSuccess( PullNRequestAndResponseHandler handler, Map<String,Value> metadata )
                    {
                        handler.pull( AUTO_PULL_SIZE );
                        handler.status = Streaming;
                    }
                },
        Failed
                {
                    void onSuccess( PullNRequestAndResponseHandler handler, Map<String,Value> metadata )
                    {
                        // you cannot be succeed if you are already failed.
                        throw new IllegalStateException( metadata.toString() );
                    }

                    void onFailure( PullNRequestAndResponseHandler handler, Throwable error )
                    {
                        handler.extractResultSummary( emptyMap() );
                        handler.afterFailure( error );

                        boolean failedRecordFuture = handler.failRecordFuture( error );
                        if ( failedRecordFuture )
                        {
                            // error propagated through the record future
                            handler.completeFailureFuture( null );
                        }
                        else
                        {
                            boolean completedFailureFuture = handler.completeFailureFuture( error );
                            if ( !completedFailureFuture )
                            {
                                // error has not been propagated to the user, remember it
                                handler.failure = error;
                            }
                        }
                    }
                },
        Cancelled
                {
                    void onRecord( PullNRequestAndResponseHandler handler, Value[] fields )
                    {
                        // discard records
                        handler.completeRecordFuture( null );
                    }
                };

        void onSuccess( PullNRequestAndResponseHandler handler, Map<String,Value> metadata )
        {
            if ( metadata.getOrDefault( "has_more", BooleanValue.FALSE ).asBoolean() )
            {
                handler.status = HasMore;
            }
            else
            {
                handler.status = Finished;
            }
            handler.status.onSuccess( handler, metadata );
        }

        void onFailure( PullNRequestAndResponseHandler handler, Throwable error )
        {
            handler.status = Failed;
            handler.status.onFailure( handler, error );
        }

        void onRecord( PullNRequestAndResponseHandler handler, Value[] fields )
        {
            throw new IllegalStateException( Arrays.toString( fields ) );
        }
    }

    public PullNRequestAndResponseHandler( Statement statement, RunResponseHandler runResponseHandler, Connection connection, MetadataExtractor metadataExtractor )
    {
        this.statement = requireNonNull( statement );
        this.runResponseHandler = requireNonNull( runResponseHandler );
        this.metadataExtractor = requireNonNull( metadataExtractor );
        this.connection = requireNonNull( connection );
        prePull();
    }

    private void prePull()
    {
        pull( AUTO_PULL_SIZE );
        this.status = Status.Streaming;
    }


    @Override
    public synchronized void onSuccess( Map<String,Value> metadata )
    {
        this.status.onSuccess( this, metadata );
    }

    protected abstract void afterSuccess( Map<String,Value> metadata );

    @Override
    public synchronized void onFailure( Throwable error )
    {
        this.status.onFailure( this, error );
    }

    protected abstract void afterFailure( Throwable error );

    @Override
    public synchronized void onRecord( Value[] fields )
    {
        this.status.onRecord( this, fields );
    }

    // whenever you call this method we pull n for you.
    // even if there is enough records buffered locally.
    // we "trust" you can use this method correctly.
    @Override
    public void pull( long size )
    {
        connection.writeAndFlush( new PullNMessage( size ), this );
    }

    @Override
    public void cancel()
    {
        if( !isCancelled )
        {
            isCancelled = true;
        }
    }

    public synchronized CompletionStage<Record> peekAsync()
    {
        Record record = records.peek();
        if ( record == null )
        {
            if ( failure != null )
            {
                return failedFuture( extractFailure() );
            }

            if ( this.status == Status.Cancelled || isFinished() )
            {
                return completedWithNull();
            }

            if ( recordFuture == null )
            {
                recordFuture = new CompletableFuture<>();
            }
            return recordFuture;
        }
        else
        {
            return completedFuture( record );
        }
    }

    public synchronized CompletionStage<Record> nextAsync()
    {
        return peekAsync().thenApply( ignore -> dequeueRecord() );
    }

    public synchronized CompletionStage<ResultSummary> summaryAsync()
    {
        return failureAsync().thenApply( error -> {
            if ( error != null )
            {
                throw Futures.asCompletionException( error );
            }
            return summary;
        } );
    }

    public synchronized CompletionStage<ResultSummary> consumeAsync()
    {
        records.clear();
        return summaryAsync();
    }

    public synchronized <T> CompletionStage<List<T>> listAsync( Function<Record,T> mapFunction )
    {
        return failureAsync().thenApply( error -> {
            if ( error != null )
            {
                throw Futures.asCompletionException( error );
            }
            return recordsAsList( mapFunction );
        } );
    }

    public synchronized CompletionStage<Throwable> failureAsync()
    {
        if ( failure != null )
        {
            return completedFuture( extractFailure() );
        }
        else if ( isFinished() )
        {
            return completedWithNull();
        }
        else
        {
            if ( failureFuture == null )
            {
                failureFuture = new CompletableFuture<>();
            }
            return failureFuture;
        }
    }

    private void enqueueRecord( Record record )
    {
        if ( records == UNINITIALIZED_RECORDS )
        {
            records = new ArrayDeque<>();
        }

        records.add( record );
    }

    private Record dequeueRecord()
    {
        return records.poll();
    }

    private <T> List<T> recordsAsList( Function<Record,T> mapFunction )
    {
        if ( !isFinished() )
        {
            throw new IllegalStateException( "Can't get records as list because SUCCESS or FAILURE did not arrive" );
        }

        List<T> result = new ArrayList<>( records.size() );
        while ( !records.isEmpty() )
        {
            Record record = records.poll();
            result.add( mapFunction.apply( record ) );
        }
        return result;
    }

    private Throwable extractFailure()
    {
        if ( failure == null )
        {
            throw new IllegalStateException( "Can't extract failure because it does not exist" );
        }

        Throwable error = failure;
        failure = null; // propagate failure only once
        return error;
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

    private boolean failRecordFuture( Throwable error )
    {
        if ( recordFuture != null )
        {
            CompletableFuture<Record> future = recordFuture;
            recordFuture = null;
            future.completeExceptionally( error );
            return true;
        }
        return false;
    }

    private boolean completeFailureFuture( Throwable error )
    {
        if ( failureFuture != null )
        {
            CompletableFuture<Throwable> future = failureFuture;
            failureFuture = null;
            future.complete( error );
            return true;
        }
        return false;
    }

    private void extractResultSummary( Map<String,Value> metadata )
    {
        long resultAvailableAfter = runResponseHandler.resultAvailableAfter();
        summary = metadataExtractor.extractSummary( statement, connection, resultAvailableAfter, metadata );
    }

    private boolean isFinished()
    {
        return this.status == Status.Finished || this.status == Status.Failed;
    }
}
