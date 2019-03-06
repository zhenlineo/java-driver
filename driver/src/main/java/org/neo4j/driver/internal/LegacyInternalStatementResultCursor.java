/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.driver.internal;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.handlers.AbstractPullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.react.result.InternalStatementResultCursor;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.exceptions.NoSuchRecordException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.util.Consumer;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.Functions;

public class LegacyInternalStatementResultCursor implements InternalStatementResultCursor
{
    private final RunResponseHandler runResponseHandler;
    private final AbstractPullAllResponseHandler pullAllHandler;

    public LegacyInternalStatementResultCursor( RunResponseHandler runResponseHandler, AbstractPullAllResponseHandler pullAllHandler )
    {
        this.runResponseHandler = runResponseHandler;
        this.pullAllHandler = pullAllHandler;
    }

    @Override
    public List<String> keys()
    {
        return runResponseHandler.statementKeys();
    }

    @Override
    public CompletionStage<ResultSummary> summaryAsync()
    {
        return pullAllHandler.summaryAsync();
    }

    @Override
    public CompletionStage<Record> nextAsync()
    {
        return pullAllHandler.nextAsync();
    }

    @Override
    public CompletionStage<Record> peekAsync()
    {
        return pullAllHandler.peekAsync();
    }

    @Override
    public CompletionStage<Record> singleAsync()
    {
        return nextAsync().thenCompose( firstRecord ->
        {
            if ( firstRecord == null )
            {
                throw new NoSuchRecordException(
                        "Cannot retrieve a single record, because this result is empty." );
            }
            return nextAsync().thenApply( secondRecord ->
            {
                if ( secondRecord != null )
                {
                    throw new NoSuchRecordException(
                            "Expected a result with a single record, but this result " +
                                    "contains at least one more. Ensure your query returns only " +
                                    "one record." );
                }
                return firstRecord;
            } );
        } );
    }

    @Override
    public CompletionStage<ResultSummary> consumeAsync()
    {
        return pullAllHandler.consumeAsync();
    }

    @Override
    public CompletionStage<ResultSummary> forEachAsync( Consumer<Record> action )
    {
        CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        internalForEachAsync( action, resultFuture );
        return resultFuture.thenCompose( ignore -> summaryAsync() );
    }

    @Override
    public CompletionStage<List<Record>> listAsync()
    {
        return listAsync( Functions.identity() );
    }

    @Override
    public <T> CompletionStage<List<T>> listAsync( Function<Record,T> mapFunction )
    {
        return pullAllHandler.listAsync( mapFunction );
    }

    @Override
    public CompletionStage<Throwable> failureAsync()
    {
        return pullAllHandler.failureAsync();
    }

    private void internalForEachAsync( Consumer<Record> action, CompletableFuture<Void> resultFuture )
    {
        CompletionStage<Record> recordFuture = nextAsync();

        // use async completion listener because of recursion, otherwise it is possible for
        // the caller thread to get StackOverflowError when result is large and buffered
        recordFuture.whenCompleteAsync( ( record, completionError ) ->
        {
            Throwable error = Futures.completionExceptionCause( completionError );
            if ( error != null )
            {
                resultFuture.completeExceptionally( error );
            }
            else if ( record != null )
            {
                try
                {
                    action.accept( record );
                }
                catch ( Throwable actionError )
                {
                    resultFuture.completeExceptionally( actionError );
                    return;
                }
                internalForEachAsync( action, resultFuture );
            }
            else
            {
                resultFuture.complete( null );
            }
        } );
    }
}
