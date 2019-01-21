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
package org.neo4j.driver.integration;

import io.netty.channel.Channel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.request.GoodbyeMessage;
import org.neo4j.driver.internal.messaging.request.HelloMessage;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.internal.util.MessageRecordingDriverFactory;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.StatementResult;
import org.neo4j.driver.StatementResultCursor;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.util.ParallelizableIT;
import org.neo4j.driver.util.SessionExtension;

import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V3;
import static org.neo4j.driver.Config.defaultConfig;
import static org.neo4j.driver.util.TestUtil.await;

@EnabledOnNeo4jWith( BOLT_V3 )
@ParallelizableIT
class SessionBoltV3IT
{
    @RegisterExtension
    static final SessionExtension session = new SessionExtension();

    @Test
    void shouldSetTransactionMetadata()
    {
        Map<String,Object> metadata = new HashMap<>();
        metadata.put( "a", "hello world" );
        metadata.put( "b", LocalDate.now() );
        metadata.put( "c", asList( true, false, true ) );

        TransactionConfig config = TransactionConfig.builder()
                .withMetadata( metadata )
                .build();

        // call listTransactions procedure that should list itself with the specified metadata
        StatementResult result = session.run( "CALL dbms.listTransactions()", config );
        Map<String,Object> receivedMetadata = result.single().get( "metaData" ).asMap();

        assertEquals( metadata, receivedMetadata );
    }

    @Test
    void shouldSetTransactionMetadataAsync()
    {
        Map<String,Object> metadata = new HashMap<>();
        metadata.put( "key1", "value1" );
        metadata.put( "key2", 42L );

        TransactionConfig config = TransactionConfig.builder()
                .withMetadata( metadata )
                .build();

        // call listTransactions procedure that should list itself with the specified metadata
        CompletionStage<Map<String,Object>> metadataFuture = session.runAsync( "CALL dbms.listTransactions()", config )
                .thenCompose( StatementResultCursor::singleAsync )
                .thenApply( record -> record.get( "metaData" ).asMap() );

        assertEquals( metadata, await( metadataFuture ) );
    }

    @Test
    void shouldSetTransactionTimeout()
    {
        // create a dummy node
        session.run( "CREATE (:Node)" ).consume();

        try ( Session otherSession = session.driver().session() )
        {
            try ( Transaction otherTx = otherSession.beginTransaction() )
            {
                // lock dummy node but keep the transaction open
                otherTx.run( "MATCH (n:Node) SET n.prop = 1" ).consume();

                TransactionConfig config = TransactionConfig.builder()
                        .withTimeout( ofMillis( 1 ) )
                        .build();

                // run a query in an auto-commit transaction with timeout and try to update the locked dummy node
                TransientException error = assertThrows( TransientException.class,
                        () -> session.run( "MATCH (n:Node) SET n.prop = 2", config ).consume() );

                assertThat( error.getMessage(), containsString( "terminated" ) );
            }
        }
    }

    @Test
    void shouldSetTransactionTimeoutAsync()
    {
        // create a dummy node
        session.run( "CREATE (:Node)" ).consume();

        try ( Session otherSession = session.driver().session() )
        {
            try ( Transaction otherTx = otherSession.beginTransaction() )
            {
                // lock dummy node but keep the transaction open
                otherTx.run( "MATCH (n:Node) SET n.prop = 1" ).consume();

                TransactionConfig config = TransactionConfig.builder()
                        .withTimeout( ofMillis( 1 ) )
                        .build();

                // run a query in an auto-commit transaction with timeout and try to update the locked dummy node
                CompletionStage<ResultSummary> resultFuture = session.runAsync( "MATCH (n:Node) SET n.prop = 2", config )
                        .thenCompose( StatementResultCursor::consumeAsync );

                TransientException error = assertThrows( TransientException.class, () -> await( resultFuture ) );

                assertThat( error.getMessage(), containsString( "terminated" ) );
            }
        }
    }

    @Test
    void shouldSetTransactionMetadataWithReadTransactionFunction()
    {
        testTransactionMetadataWithTransactionFunctions( true );
    }

    @Test
    void shouldSetTransactionMetadataWithWriteTransactionFunction()
    {
        testTransactionMetadataWithTransactionFunctions( false );
    }

    @Test
    void shouldSetTransactionMetadataWithAsyncReadTransactionFunction()
    {
        testTransactionMetadataWithAsyncTransactionFunctions( true );
    }

    @Test
    void shouldSetTransactionMetadataWithAsyncWriteTransactionFunction()
    {
        testTransactionMetadataWithAsyncTransactionFunctions( false );
    }

    @Test
    void shouldUseBookmarksForAutoCommitTransactions()
    {
        String initialBookmark = session.lastBookmark();

        session.run( "CREATE ()" ).consume();
        String bookmark1 = session.lastBookmark();
        assertNotNull( bookmark1 );
        assertNotEquals( initialBookmark, bookmark1 );

        session.run( "CREATE ()" ).consume();
        String bookmark2 = session.lastBookmark();
        assertNotNull( bookmark2 );
        assertNotEquals( initialBookmark, bookmark2 );
        assertNotEquals( bookmark1, bookmark2 );

        session.run( "CREATE ()" ).consume();
        String bookmark3 = session.lastBookmark();
        assertNotNull( bookmark3 );
        assertNotEquals( initialBookmark, bookmark3 );
        assertNotEquals( bookmark1, bookmark3 );
        assertNotEquals( bookmark2, bookmark3 );
    }

    @Test
    void shouldUseBookmarksForAutoCommitAndExplicitTransactions()
    {
        String initialBookmark = session.lastBookmark();

        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( "CREATE ()" );
            tx.success();
        }
        String bookmark1 = session.lastBookmark();
        assertNotNull( bookmark1 );
        assertNotEquals( initialBookmark, bookmark1 );

        session.run( "CREATE ()" ).consume();
        String bookmark2 = session.lastBookmark();
        assertNotNull( bookmark2 );
        assertNotEquals( initialBookmark, bookmark2 );
        assertNotEquals( bookmark1, bookmark2 );

        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( "CREATE ()" );
            tx.success();
        }
        String bookmark3 = session.lastBookmark();
        assertNotNull( bookmark3 );
        assertNotEquals( initialBookmark, bookmark3 );
        assertNotEquals( bookmark1, bookmark3 );
        assertNotEquals( bookmark2, bookmark3 );
    }

    @Test
    void shouldUseBookmarksForAutoCommitTransactionsAndTransactionFunctions()
    {
        String initialBookmark = session.lastBookmark();

        session.writeTransaction( tx -> tx.run( "CREATE ()" ) );
        String bookmark1 = session.lastBookmark();
        assertNotNull( bookmark1 );
        assertNotEquals( initialBookmark, bookmark1 );

        session.run( "CREATE ()" ).consume();
        String bookmark2 = session.lastBookmark();
        assertNotNull( bookmark2 );
        assertNotEquals( initialBookmark, bookmark2 );
        assertNotEquals( bookmark1, bookmark2 );

        session.writeTransaction( tx -> tx.run( "CREATE ()" ) );
        String bookmark3 = session.lastBookmark();
        assertNotNull( bookmark3 );
        assertNotEquals( initialBookmark, bookmark3 );
        assertNotEquals( bookmark1, bookmark3 );
        assertNotEquals( bookmark2, bookmark3 );
    }

    @Test
    void shouldSendGoodbyeWhenClosingDriver()
    {
        int txCount = 13;
        MessageRecordingDriverFactory driverFactory = new MessageRecordingDriverFactory();

        try ( Driver driver = driverFactory.newInstance( session.uri(), session.authToken(), RoutingSettings.DEFAULT, RetrySettings.DEFAULT, defaultConfig() ) )
        {
            List<Session> sessions = new ArrayList<>();
            List<Transaction> txs = new ArrayList<>();
            for ( int i = 0; i < txCount; i++ )
            {
                Session session = driver.session();
                sessions.add( session );
                Transaction tx = session.beginTransaction();
                txs.add( tx );
            }

            for ( int i = 0; i < txCount; i++ )
            {
                Session session = sessions.get( i );
                Transaction tx = txs.get( i );

                tx.run( "CREATE ()" );
                tx.success();
                tx.close();
                session.close();
            }
        }

        Map<Channel,List<Message>> messagesByChannel = driverFactory.getMessagesByChannel();
        assertEquals( txCount, messagesByChannel.size() );

        for ( List<Message> messages : messagesByChannel.values() )
        {
            assertThat( messages.size(), greaterThan( 2 ) );
            assertThat( messages.get( 0 ), instanceOf( HelloMessage.class ) ); // first message is HELLO
            assertThat( messages.get( messages.size() - 1 ), instanceOf( GoodbyeMessage.class ) ); // last message is GOODBYE
        }
    }

    private static void testTransactionMetadataWithTransactionFunctions( boolean read )
    {
        Map<String,Object> metadata = new HashMap<>();
        metadata.put( "foo", "bar" );
        metadata.put( "baz", true );
        metadata.put( "qux", 12345L );

        TransactionConfig config = TransactionConfig.builder()
                .withMetadata( metadata )
                .build();

        // call listTransactions procedure that should list itself with the specified metadata
        StatementResult result = read ? session.readTransaction( tx -> tx.run( "CALL dbms.listTransactions()" ), config )
                                      : session.writeTransaction( tx -> tx.run( "CALL dbms.listTransactions()" ), config );

        Map<String,Object> receivedMetadata = result.single().get( "metaData" ).asMap();

        assertEquals( metadata, receivedMetadata );
    }

    private static void testTransactionMetadataWithAsyncTransactionFunctions( boolean read )
    {
        Map<String,Object> metadata = new HashMap<>();
        metadata.put( "foo", "bar" );
        metadata.put( "baz", true );
        metadata.put( "qux", 12345L );

        TransactionConfig config = TransactionConfig.builder()
                .withMetadata( metadata )
                .build();

        // call listTransactions procedure that should list itself with the specified metadata
        CompletionStage<StatementResultCursor> cursorFuture =
                read ? session.readTransactionAsync( tx -> tx.runAsync( "CALL dbms.listTransactions()" ), config )
                     : session.writeTransactionAsync( tx -> tx.runAsync( "CALL dbms.listTransactions()" ), config );

        CompletionStage<Map<String,Object>> metadataFuture = cursorFuture.thenCompose( StatementResultCursor::singleAsync )
                .thenApply( record -> record.get( "metaData" ).asMap() );

        assertEquals( metadata, await( metadataFuture ) );
    }

}
