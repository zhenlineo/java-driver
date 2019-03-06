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
package org.neo4j.driver.internal.messaging.v1;

import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.Bookmarks;
import org.neo4j.driver.internal.BookmarksHolder;
import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.LegacyInternalStatementResultCursor;
import org.neo4j.driver.internal.async.ChannelAttributes;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.handlers.BeginTxResponseHandler;
import org.neo4j.driver.internal.handlers.CommitTxResponseHandler;
import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.handlers.RollbackTxResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.SessionPullAllResponseHandler;
import org.neo4j.driver.internal.handlers.TransactionPullAllResponseHandler;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.request.InitMessage;
import org.neo4j.driver.internal.messaging.request.PullAllMessage;
import org.neo4j.driver.internal.messaging.request.RunMessage;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.TransactionConfig;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.util.Futures.blockingGet;
import static org.neo4j.driver.v1.Values.value;
import static org.neo4j.driver.v1.util.TestUtil.DEFAULT_TEST_PROTOCOL;
import static org.neo4j.driver.v1.util.TestUtil.await;
import static org.neo4j.driver.v1.util.TestUtil.connectionMock;

public class BoltProtocolV1Test
{
    private static final String QUERY = "RETURN $x";
    private static final Map<String,Value> PARAMS = singletonMap( "x", value( 42 ) );
    private static final Statement STATEMENT = new Statement( QUERY, value( PARAMS ) );

    private final BoltProtocol protocol = createProtocol();
    private final EmbeddedChannel channel = new EmbeddedChannel();
    private final InboundMessageDispatcher messageDispatcher = new InboundMessageDispatcher( channel, Logging.none() );

    @BeforeEach
    void beforeEach()
    {
        ChannelAttributes.setMessageDispatcher( channel, messageDispatcher );
    }

    @AfterEach
    void afterEach()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldCreateMessageFormat()
    {
        assertThat( protocol.createMessageFormat(), instanceOf( expectedMessageFormatType() ) );
    }

    @Test
    void shouldInitializeChannel()
    {
        ChannelPromise promise = channel.newPromise();

        protocol.initializeChannel( "MyDriver/5.3", dummyAuthToken(), promise );

        assertThat( channel.outboundMessages(), hasSize( 1 ) );
        assertThat( channel.outboundMessages().poll(), instanceOf( InitMessage.class ) );
        assertEquals( 1, messageDispatcher.queuedHandlersCount() );
        assertFalse( promise.isDone() );

        messageDispatcher.handleSuccessMessage( singletonMap( "server", value( "Neo4j/3.1.0" ) ) );

        assertTrue( promise.isDone() );
        assertTrue( promise.isSuccess() );
    }

    @Test
    void shouldFailToInitializeChannelWhenErrorIsReceived()
    {
        ChannelPromise promise = channel.newPromise();

        protocol.initializeChannel( "MyDriver/3.1", dummyAuthToken(), promise );

        assertThat( channel.outboundMessages(), hasSize( 1 ) );
        assertThat( channel.outboundMessages().poll(), instanceOf( InitMessage.class ) );
        assertEquals( 1, messageDispatcher.queuedHandlersCount() );
        assertFalse( promise.isDone() );

        messageDispatcher.handleFailureMessage( "Neo.TransientError.General.DatabaseUnavailable", "Oh no!" );

        assertTrue( promise.isDone() );
        assertFalse( promise.isSuccess() );
    }

    @Test
    void shouldBeginTransactionWithoutBookmark()
    {
        Connection connection = connectionMock();

        CompletionStage<Void> stage = protocol.beginTransaction( connection, Bookmarks.empty(), TransactionConfig.empty() );

        verify( connection ).write(
                new RunMessage( "BEGIN" ), NoOpResponseHandler.INSTANCE,
                PullAllMessage.PULL_ALL, NoOpResponseHandler.INSTANCE );

        assertNull( blockingGet( stage ) );
    }

    @Test
    void shouldBeginTransactionWithBookmarks()
    {
        Connection connection = connectionMock();
        Bookmarks bookmarks = Bookmarks.from( "neo4j:bookmark:v1:tx100" );

        CompletionStage<Void> stage = protocol.beginTransaction( connection, bookmarks, TransactionConfig.empty() );

        verify( connection ).writeAndFlush(
                eq( new RunMessage( "BEGIN", bookmarks.asBeginTransactionParameters() ) ), eq( NoOpResponseHandler.INSTANCE ),
                eq( PullAllMessage.PULL_ALL ), any( BeginTxResponseHandler.class ) );

        assertNull( Futures.blockingGet( stage ) );
    }

    @Test
    void shouldCommitTransaction()
    {
        String bookmarkString = "neo4j:bookmark:v1:tx1909";

        Connection connection = mock( Connection.class );
        when( connection.protocol() ).thenReturn( DEFAULT_TEST_PROTOCOL );
        doAnswer( invocation ->
        {
            ResponseHandler commitHandler = invocation.getArgument( 3 );
            commitHandler.onSuccess( singletonMap( "bookmark", value( bookmarkString ) ) );
            return null;
        } ).when( connection ).writeAndFlush( eq( new RunMessage( "COMMIT" ) ), any(), any(), any() );

        CompletionStage<Bookmarks> stage = protocol.commitTransaction( connection );

        verify( connection ).writeAndFlush(
                eq( new RunMessage( "COMMIT" ) ), eq( NoOpResponseHandler.INSTANCE ),
                eq( PullAllMessage.PULL_ALL ), any( CommitTxResponseHandler.class ) );

        assertEquals( Bookmarks.from( bookmarkString ), await( stage ) );
    }

    @Test
    void shouldRollbackTransaction()
    {
        Connection connection = connectionMock();

        CompletionStage<Void> stage = protocol.rollbackTransaction( connection );

        verify( connection ).writeAndFlush(
                eq( new RunMessage( "ROLLBACK" ) ), eq( NoOpResponseHandler.INSTANCE ),
                eq( PullAllMessage.PULL_ALL ), any( RollbackTxResponseHandler.class ) );

        assertNull( Futures.blockingGet( stage ) );
    }

    @Test
    void shouldRunInAutoCommitTransactionWithoutWaitingForRunResponse() throws Exception
    {
        testRunWithoutWaitingForRunResponse( true );
    }

    @Test
    void shouldRunInAutoCommitTransactionAndWaitForSuccessRunResponse() throws Exception
    {
        testRunWithWaitingForResponse( true, true );
    }

    @Test
    void shouldRunInAutoCommitTransactionAndWaitForFailureRunResponse() throws Exception
    {
        testRunWithWaitingForResponse( false, true );
    }

    @Test
    void shouldRunInTransactionWithoutWaitingForRunResponse() throws Exception
    {
        testRunWithoutWaitingForRunResponse( false );
    }

    @Test
    void shouldRunInTransactionAndWaitForSuccessRunResponse() throws Exception
    {
        testRunWithWaitingForResponse( true, false );
    }

    @Test
    void shouldRunInTransactionAndWaitForFailureRunResponse() throws Exception
    {
        testRunWithWaitingForResponse( false, false );
    }

    @Test
    void shouldNotSupportTransactionConfigInBeginTransaction()
    {
        TransactionConfig config = TransactionConfig.builder()
                .withTimeout( Duration.ofSeconds( 5 ) )
                .withMetadata( singletonMap( "key", "value" ) )
                .build();

        CompletionStage<Void> txStage = protocol.beginTransaction( connectionMock(), Bookmarks.empty(), config );

        ClientException e = assertThrows( ClientException.class, () -> await( txStage ) );
        assertThat( e.getMessage(), startsWith( "Driver is connected to the database that does not support transaction configuration" ) );
    }

    @Test
    void shouldNotSupportTransactionConfigForAutoCommitTransactions()
    {
        TransactionConfig config = TransactionConfig.builder()
                .withTimeout( Duration.ofSeconds( 42 ) )
                .withMetadata( singletonMap( "hello", "world" ) )
                .build();

        CompletionStage<LegacyInternalStatementResultCursor> cursorFuture = protocol.runInAutoCommitTransaction( connectionMock(), new Statement( "RETURN 1" ),
                BookmarksHolder.NO_OP, config, true ).thenApply( cursorFactory -> (LegacyInternalStatementResultCursor) cursorFactory.asyncResult() );;

        ClientException e = assertThrows( ClientException.class, () -> await( cursorFuture ) );
        assertThat( e.getMessage(), startsWith( "Driver is connected to the database that does not support transaction configuration" ) );
    }

    protected BoltProtocol createProtocol()
    {
        return BoltProtocolV1.INSTANCE;
    }

    protected Class<? extends MessageFormat> expectedMessageFormatType()
    {
        return MessageFormatV1.class;
    }

    private void testRunWithoutWaitingForRunResponse( boolean autoCommitTx ) throws Exception
    {
        Connection connection = mock( Connection.class );

        CompletionStage<LegacyInternalStatementResultCursor> cursorStage;
        if ( autoCommitTx )
        {

            cursorStage = protocol.runInAutoCommitTransaction( connection, STATEMENT, BookmarksHolder.NO_OP, TransactionConfig.empty(), false )
                    .thenApply( cursorFactory -> (LegacyInternalStatementResultCursor) cursorFactory.asyncResult() );
        }
        else
        {
            cursorStage = protocol.runInExplicitTransaction( connection, STATEMENT, mock( ExplicitTransaction.class ), false )
                    .thenApply( cursorFactory -> (LegacyInternalStatementResultCursor) cursorFactory.asyncResult() );;
        }
        CompletableFuture<LegacyInternalStatementResultCursor> cursorFuture = cursorStage.toCompletableFuture();

        assertTrue( cursorFuture.isDone() );
        assertNotNull( cursorFuture.get() );
        verifyRunInvoked( connection, autoCommitTx );
    }

    private void testRunWithWaitingForResponse( boolean success, boolean session ) throws Exception
    {
        Connection connection = mock( Connection.class );

        CompletionStage<LegacyInternalStatementResultCursor> cursorStage;
        if ( session )
        {
            cursorStage = protocol.runInAutoCommitTransaction( connection, STATEMENT, BookmarksHolder.NO_OP, TransactionConfig.empty(), true )
                    .thenApply( cursorFactory -> (LegacyInternalStatementResultCursor) cursorFactory.asyncResult() );;
        }
        else
        {
            cursorStage = protocol.runInExplicitTransaction( connection, STATEMENT, mock( ExplicitTransaction.class ), true )
                    .thenApply( cursorFactory -> (LegacyInternalStatementResultCursor) cursorFactory.asyncResult() );;
        }
        CompletableFuture<LegacyInternalStatementResultCursor> cursorFuture = cursorStage.toCompletableFuture();

        assertFalse( cursorFuture.isDone() );
        ResponseHandler runResponseHandler = verifyRunInvoked( connection, session );

        if ( success )
        {
            runResponseHandler.onSuccess( emptyMap() );
        }
        else
        {
            runResponseHandler.onFailure( new RuntimeException() );
        }

        assertTrue( cursorFuture.isDone() );
        assertNotNull( cursorFuture.get() );
    }

    private static ResponseHandler verifyRunInvoked( Connection connection, boolean session )
    {
        ArgumentCaptor<ResponseHandler> runHandlerCaptor = ArgumentCaptor.forClass( ResponseHandler.class );
        ArgumentCaptor<ResponseHandler> pullAllHandlerCaptor = ArgumentCaptor.forClass( ResponseHandler.class );

        verify( connection ).writeAndFlush( eq( new RunMessage( QUERY, PARAMS ) ), runHandlerCaptor.capture(),
                eq( PullAllMessage.PULL_ALL ), pullAllHandlerCaptor.capture() );

        assertThat( runHandlerCaptor.getValue(), instanceOf( RunResponseHandler.class ) );

        if ( session )
        {
            assertThat( pullAllHandlerCaptor.getValue(), instanceOf( SessionPullAllResponseHandler.class ) );
        }
        else
        {
            assertThat( pullAllHandlerCaptor.getValue(), instanceOf( TransactionPullAllResponseHandler.class ) );
        }

        return runHandlerCaptor.getValue();
    }

    private static Map<String,Value> dummyAuthToken()
    {
        Map<String,Value> authToken = new HashMap<>();
        authToken.put( "username", value( "hello" ) );
        authToken.put( "password", value( "world" ) );
        return authToken;
    }
}
