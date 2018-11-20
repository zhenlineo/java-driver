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
package org.neo4j.driver.react.result;

import reactor.core.publisher.Flux;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.util.Consumer;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.Functions;

public class AsyncStatementResultCursor implements InternalStatementResultCursor
{
    private static final int RECORD_BUFFER_HIGH_WATERMARK = Integer.getInteger( "recordBufferHighWatermark", 1000 );

    private final RunResponseHandler runResponseHandler;

    private final CompletableFuture<ResultSummary> summaryFuture = new CompletableFuture<>();
    private final BasicPullResponseHandler pullResponseHandler;
    private Flux<Record> recordFlux;

    public AsyncStatementResultCursor( RunResponseHandler runResponseHandler, BasicPullResponseHandler pullResponseHandler )
    {
        this.runResponseHandler = runResponseHandler;
        this.pullResponseHandler = pullResponseHandler;
        pullResponseHandler.installSummaryConsumer( ( summary, error ) -> {
            if ( summary != null )
            {
                summaryFuture.complete( summary );
            }
            else if ( error != null )
            {
                summaryFuture.completeExceptionally( error );
            }
        } );

        recordFlux = Flux.create( sink -> {
//            sink.onCancel( pullResponseHandler::cancel );
            pullResponseHandler.installRecordConsumer( ( record, throwable ) -> {
                if ( record != null )
                {
                    sink.next( record );
                }
                else if ( throwable != null )
                {
                    sink.error( throwable );
                }
                else
                {
                    sink.complete();
                }
            } );
            sink.onRequest( pullResponseHandler::request );
        } );

        recordFlux = recordFlux.limitRate( RECORD_BUFFER_HIGH_WATERMARK );
    }

    @Override
    public List<String> keys()
    {
        return runResponseHandler.statementKeys();
    }

    @Override
    public CompletionStage<ResultSummary> summaryAsync()
    {
        recordFlux = recordFlux.cache();
        recordFlux.subscribe();
        return summaryFuture;
    }

    @Override
    public CompletionStage<Record> nextAsync()
    {
        return recordFlux..next().toFuture();
//        if ( peeked != null )
//        {
//            CompletableFuture<Record> toReturn = this.peeked;
//            this.peeked = null;
//            return toReturn;
//        }
//        else
//        {
//            return recordFlux.next().toFuture();
//        }
    }

    @Override
    public CompletionStage<Record> peekAsync()
    {
//        if ( peeked == null )
//        {
//            peeked = recordFlux.next().toFuture();
//        }
//        return peeked;
        return Futures.completedWithNull();
    }

    @Override
    public CompletionStage<Record> singleAsync()
    {
        return recordFlux.single().toFuture();
    }

    @Override
    public CompletionStage<ResultSummary> consumeAsync()
    {
        pullResponseHandler.cancel();
        return summaryAsync();
    }

    @Override
    public CompletionStage<ResultSummary> forEachAsync( Consumer<Record> action )
    {
        recordFlux = recordFlux.doOnNext( action::accept );
        recordFlux.subscribe();
        return summaryAsync();
    }

    @Override
    public CompletionStage<List<Record>> listAsync()
    {
        return listAsync( Functions.identity() );
    }

    @Override
    public <T> CompletableFuture<List<T>> listAsync( Function<Record,T> mapFunction )
    {
        return recordFlux.map( mapFunction::apply ).collectList().toFuture();
//        List<T> values = new ArrayList<>();
//        recordFlux = recordFlux.doOnNext( record -> {
//            values.add( mapFunction.apply( record ) );
//        } );
//        recordFlux.subscribe();
//
//        return failureAsync().thenApply( error -> {
//            if ( error != null )
//            {
//                throw Futures.asCompletionException( error );
//            }
//            else
//            {
//                return values;
//            }
//        } );
    }

    public CompletionStage<Throwable> failureAsync()
    {
        return summaryAsync().thenApply( summary -> (Throwable) null ).exceptionally( error -> error );
    }

    public CompletionStage<Boolean> hasNext()
    {
        return recordFlux.hasElements().toFuture();
    }
}
