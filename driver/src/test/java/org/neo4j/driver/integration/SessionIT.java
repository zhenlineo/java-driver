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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.util.DriverFactoryWithFixedRetryLogic;
import org.neo4j.driver.internal.util.DriverFactoryWithOneEventLoopThread;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.StatementResult;
import org.neo4j.driver.StatementRunner;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.StatementType;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;
import org.neo4j.driver.util.TestUtil;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.Matchers.arithmeticError;
import static org.neo4j.driver.internal.util.Matchers.connectionAcquisitionTimeoutError;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOOKMARKS;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.util.DaemonThreadFactory.daemon;
import static org.neo4j.driver.util.Neo4jRunner.DEFAULT_AUTH_TOKEN;

@ParallelizableIT
class SessionIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private Driver driver;
    private ExecutorService executor;

    @AfterEach
    void tearDown()
    {
        if ( driver != null )
        {
            driver.close();
        }
        if ( executor != null )
        {
            executor.shutdownNow();
        }
    }

    @Test
    void shouldKnowSessionIsClosed()
    {
        // Given
        Session session = neo4j.driver().session();

        // When
        session.close();

        // Then
        assertFalse( session.isOpen() );
    }

    @Test
    void shouldHandleNullConfig()
    {
        // Given
        driver = GraphDatabase.driver( neo4j.uri(), neo4j.authToken(), null );
        Session session = driver.session();

        // When
        session.close();

        // Then
        assertFalse( session.isOpen() );
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    void shouldHandleNullAuthToken()
    {
        AuthToken token = null;

        // null auth token should be interpreted as AuthTokens.none() and fail driver creation
        // because server expects basic auth
        assertThrows( AuthenticationException.class, () -> GraphDatabase.driver( neo4j.uri(), token ) );
    }

    @Test
    void executeReadTxInReadSession()
    {
        testExecuteReadTx( AccessMode.READ );
    }

    @Test
    void executeReadTxInWriteSession()
    {
        testExecuteReadTx( AccessMode.WRITE );
    }

    @Test
    void executeWriteTxInReadSession()
    {
        testExecuteWriteTx( AccessMode.READ );
    }

    @Test
    void executeWriteTxInWriteSession()
    {
        testExecuteWriteTx( AccessMode.WRITE );
    }

    @Test
    void rollsBackWriteTxInReadSessionWhenFunctionThrows()
    {
        testTxRollbackWhenFunctionThrows( AccessMode.READ );
    }

    @Test
    void rollsBackWriteTxInWriteSessionWhenFunctionThrows()
    {
        testTxRollbackWhenFunctionThrows( AccessMode.WRITE );
    }

    @Test
    void readTxRetriedUntilSuccess()
    {
        int failures = 6;
        int retries = failures + 1;
        try ( Driver driver = newDriverWithFixedRetries( retries ) )
        {
            try ( Session session = driver.session() )
            {
                session.run( "CREATE (:Person {name: 'Bruce Banner'})" );
            }

            ThrowingWork work = newThrowingWorkSpy( "MATCH (n) RETURN n.name", failures );
            try ( Session session = driver.session() )
            {
                Record record = session.readTransaction( work );
                assertEquals( "Bruce Banner", record.get( 0 ).asString() );
            }

            verify( work, times( retries ) ).execute( any( Transaction.class ) );
        }
    }

    @Test
    void writeTxRetriedUntilSuccess()
    {
        int failures = 4;
        int retries = failures + 1;
        try ( Driver driver = newDriverWithFixedRetries( retries ) )
        {
            ThrowingWork work = newThrowingWorkSpy( "CREATE (p:Person {name: 'Hulk'}) RETURN p", failures );
            try ( Session session = driver.session() )
            {
                Record record = session.writeTransaction( work );
                assertEquals( "Hulk", record.get( 0 ).asNode().get( "name" ).asString() );
            }

            try ( Session session = driver.session() )
            {
                Record record = session.run( "MATCH (p: Person {name: 'Hulk'}) RETURN count(p)" ).single();
                assertEquals( 1, record.get( 0 ).asInt() );
            }

            verify( work, times( retries ) ).execute( any( Transaction.class ) );
        }
    }

    @Test
    void readTxRetriedUntilFailure()
    {
        int failures = 3;
        int retries = failures - 1;
        try ( Driver driver = newDriverWithFixedRetries( retries ) )
        {
            ThrowingWork work = newThrowingWorkSpy( "MATCH (n) RETURN n.name", failures );
            try ( Session session = driver.session() )
            {
                assertThrows( ServiceUnavailableException.class, () -> session.readTransaction( work ) );
            }

            verify( work, times( failures ) ).execute( any( Transaction.class ) );
        }
    }

    @Test
    void writeTxRetriedUntilFailure()
    {
        int failures = 8;
        int retries = failures - 1;
        try ( Driver driver = newDriverWithFixedRetries( retries ) )
        {
            ThrowingWork work = newThrowingWorkSpy( "CREATE (:Person {name: 'Ronan'})", failures );
            try ( Session session = driver.session() )
            {
                assertThrows( ServiceUnavailableException.class, () -> session.writeTransaction( work ) );
            }

            try ( Session session = driver.session() )
            {
                StatementResult result = session.run( "MATCH (p:Person {name: 'Ronan'}) RETURN count(p)" );
                assertEquals( 0, result.single().get( 0 ).asInt() );
            }

            verify( work, times( failures ) ).execute( any( Transaction.class ) );
        }
    }

    @Test
    void writeTxRetryErrorsAreCollected()
    {
        try ( Driver driver = newDriverWithLimitedRetries( 5, TimeUnit.SECONDS ) )
        {
            ThrowingWork work = newThrowingWorkSpy( "CREATE (:Person {name: 'Ronan'})", Integer.MAX_VALUE );
            int suppressedErrors;
            try ( Session session = driver.session() )
            {
                ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class, () -> session.writeTransaction( work ) );
                assertThat( e.getSuppressed(), not( emptyArray() ) );
                suppressedErrors = e.getSuppressed().length;
            }

            try ( Session session = driver.session() )
            {
                StatementResult result = session.run( "MATCH (p:Person {name: 'Ronan'}) RETURN count(p)" );
                assertEquals( 0, result.single().get( 0 ).asInt() );
            }

            verify( work, times( suppressedErrors + 1 ) ).execute( any( Transaction.class ) );
        }
    }

    @Test
    void readTxRetryErrorsAreCollected()
    {
        try ( Driver driver = newDriverWithLimitedRetries( 4, TimeUnit.SECONDS ) )
        {
            ThrowingWork work = newThrowingWorkSpy( "MATCH (n) RETURN n.name", Integer.MAX_VALUE );
            int suppressedErrors;
            try ( Session session = driver.session() )
            {
                ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class, () -> session.readTransaction( work ) );
                assertThat( e.getSuppressed(), not( emptyArray() ) );
                suppressedErrors = e.getSuppressed().length;
            }

            verify( work, times( suppressedErrors + 1 ) ).execute( any( Transaction.class ) );
        }
    }

    @Test
    @EnabledOnNeo4jWith( BOOKMARKS )
    void readTxCommittedWithoutTxSuccess()
    {
        try ( Driver driver = newDriverWithoutRetries();
              Session session = driver.session() )
        {
            assertNull( session.lastBookmark() );

            long answer = session.readTransaction( tx -> tx.run( "RETURN 42" ).single().get( 0 ).asLong() );
            assertEquals( 42, answer );

            // bookmark should be not-null after commit
            assertNotNull( session.lastBookmark() );
        }
    }

    @Test
    void writeTxCommittedWithoutTxSuccess()
    {
        try ( Driver driver = newDriverWithoutRetries() )
        {
            try ( Session session = driver.session() )
            {
                long answer = session.writeTransaction( tx ->
                        tx.run( "CREATE (:Person {name: 'Thor Odinson'}) RETURN 42" ).single().get( 0 ).asLong() );
                assertEquals( 42, answer );
            }

            try ( Session session = driver.session() )
            {
                StatementResult result = session.run( "MATCH (p:Person {name: 'Thor Odinson'}) RETURN count(p)" );
                assertEquals( 1, result.single().get( 0 ).asInt() );
            }
        }
    }

    @Test
    @EnabledOnNeo4jWith( BOOKMARKS )
    void readTxRolledBackWithTxFailure()
    {
        try ( Driver driver = newDriverWithoutRetries();
              Session session = driver.session() )
        {
            assertNull( session.lastBookmark() );

            long answer = session.readTransaction( tx ->
            {
                StatementResult result = tx.run( "RETURN 42" );
                tx.failure();
                return result.single().get( 0 ).asLong();
            } );
            assertEquals( 42, answer );

            // bookmark should remain null after rollback
            assertNull( session.lastBookmark() );
        }
    }

    @Test
    void writeTxRolledBackWithTxFailure()
    {
        try ( Driver driver = newDriverWithoutRetries() )
        {
            try ( Session session = driver.session() )
            {
                int answer = session.writeTransaction( tx ->
                {
                    tx.run( "CREATE (:Person {name: 'Natasha Romanoff'})" );
                    tx.failure();
                    return 42;
                } );

                assertEquals( 42, answer );
            }

            try ( Session session = driver.session() )
            {
                StatementResult result = session.run( "MATCH (p:Person {name: 'Natasha Romanoff'}) RETURN count(p)" );
                assertEquals( 0, result.single().get( 0 ).asInt() );
            }
        }
    }

    @Test
    @EnabledOnNeo4jWith( BOOKMARKS )
    void readTxRolledBackWhenExceptionIsThrown()
    {
        try ( Driver driver = newDriverWithoutRetries();
              Session session = driver.session() )
        {
            assertNull( session.lastBookmark() );

            assertThrows( IllegalStateException.class, () ->
                    session.readTransaction( tx ->
                    {
                        StatementResult result = tx.run( "RETURN 42" );
                        if ( result.single().get( 0 ).asLong() == 42 )
                        {
                            throw new IllegalStateException();
                        }
                        return 1L;
                    } ) );

            // bookmark should remain null after rollback
            assertNull( session.lastBookmark() );
        }
    }

    @Test
    void writeTxRolledBackWhenExceptionIsThrown()
    {
        try ( Driver driver = newDriverWithoutRetries() )
        {
            try ( Session session = driver.session() )
            {
                assertThrows( IllegalStateException.class, () ->
                        session.writeTransaction( tx ->
                        {
                            tx.run( "CREATE (:Person {name: 'Loki Odinson'})" );
                            throw new IllegalStateException();
                        } ) );
            }

            try ( Session session = driver.session() )
            {
                StatementResult result = session.run( "MATCH (p:Person {name: 'Natasha Romanoff'}) RETURN count(p)" );
                assertEquals( 0, result.single().get( 0 ).asInt() );
            }
        }
    }

    @Test
    @EnabledOnNeo4jWith( BOOKMARKS )
    void readTxRolledBackWhenMarkedBothSuccessAndFailure()
    {
        try ( Driver driver = newDriverWithoutRetries();
              Session session = driver.session() )
        {
            assertNull( session.lastBookmark() );

            long answer = session.readTransaction( tx ->
            {
                StatementResult result = tx.run( "RETURN 42" );
                tx.success();
                tx.failure();
                return result.single().get( 0 ).asLong();
            } );
            assertEquals( 42, answer );

            // bookmark should remain null after rollback
            assertNull( session.lastBookmark() );
        }
    }

    @Test
    void writeTxRolledBackWhenMarkedBothSuccessAndFailure()
    {
        try ( Driver driver = newDriverWithoutRetries() )
        {
            try ( Session session = driver.session() )
            {
                int answer = session.writeTransaction( tx ->
                {
                    tx.run( "CREATE (:Person {name: 'Natasha Romanoff'})" );
                    tx.success();
                    tx.failure();
                    return 42;
                } );

                assertEquals( 42, answer );
            }

            try ( Session session = driver.session() )
            {
                StatementResult result = session.run( "MATCH (p:Person {name: 'Natasha Romanoff'}) RETURN count(p)" );
                assertEquals( 0, result.single().get( 0 ).asInt() );
            }
        }
    }

    @Test
    @EnabledOnNeo4jWith( BOOKMARKS )
    void readTxRolledBackWhenMarkedAsSuccessAndThrowsException()
    {
        try ( Driver driver = newDriverWithoutRetries();
              Session session = driver.session() )
        {
            assertNull( session.lastBookmark() );

            assertThrows( IllegalStateException.class, () ->
                    session.readTransaction( tx ->
                    {
                        tx.run( "RETURN 42" );
                        tx.success();
                        throw new IllegalStateException();
                    } ) );

            // bookmark should remain null after rollback
            assertNull( session.lastBookmark() );
        }
    }

    @Test
    void writeTxRolledBackWhenMarkedAsSuccessAndThrowsException()
    {
        try ( Driver driver = newDriverWithoutRetries() )
        {
            try ( Session session = driver.session() )
            {
                assertThrows( IllegalStateException.class, () ->
                        session.writeTransaction( tx ->
                        {
                            tx.run( "CREATE (:Person {name: 'Natasha Romanoff'})" );
                            tx.success();
                            throw new IllegalStateException();
                        } ) );
            }

            try ( Session session = driver.session() )
            {
                StatementResult result = session.run( "MATCH (p:Person {name: 'Natasha Romanoff'}) RETURN count(p)" );
                assertEquals( 0, result.single().get( 0 ).asInt() );
            }
        }
    }

    @Test
    void transactionRunShouldFailOnDeadlocks() throws Exception
    {
        final int nodeId1 = 42;
        final int nodeId2 = 4242;
        final int newNodeId1 = 1;
        final int newNodeId2 = 2;

        createNodeWithId( nodeId1 );
        createNodeWithId( nodeId2 );

        final CountDownLatch latch1 = new CountDownLatch( 1 );
        final CountDownLatch latch2 = new CountDownLatch( 1 );

        Future<Void> result1 = executeInDifferentThread( () ->
        {
            try ( Session session = neo4j.driver().session();
                  Transaction tx = session.beginTransaction() )
            {
                // lock first node
                updateNodeId( tx, nodeId1, newNodeId1 ).consume();

                latch1.await();
                latch2.countDown();

                // lock second node
                updateNodeId( tx, nodeId2, newNodeId1 ).consume();

                tx.success();
            }
            return null;
        } );

        Future<Void> result2 = executeInDifferentThread( () ->
        {
            try ( Session session = neo4j.driver().session();
                  Transaction tx = session.beginTransaction() )
            {
                // lock second node
                updateNodeId( tx, nodeId2, newNodeId2 ).consume();

                latch1.countDown();
                latch2.await();

                // lock first node
                updateNodeId( tx, nodeId1, newNodeId2 ).consume();

                tx.success();
            }
            return null;
        } );

        boolean firstResultFailed = assertOneOfTwoFuturesFailWithDeadlock( result1, result2 );
        if ( firstResultFailed )
        {
            assertEquals( 0, countNodesWithId( newNodeId1 ) );
            assertEquals( 2, countNodesWithId( newNodeId2 ) );
        }
        else
        {
            assertEquals( 2, countNodesWithId( newNodeId1 ) );
            assertEquals( 0, countNodesWithId( newNodeId2 ) );
        }
    }

    @Test
    void writeTransactionFunctionShouldRetryDeadlocks() throws Exception
    {
        final int nodeId1 = 42;
        final int nodeId2 = 4242;
        final int nodeId3 = 424242;
        final int newNodeId1 = 1;
        final int newNodeId2 = 2;

        createNodeWithId( nodeId1 );
        createNodeWithId( nodeId2 );

        final CountDownLatch latch1 = new CountDownLatch( 1 );
        final CountDownLatch latch2 = new CountDownLatch( 1 );

        Future<Void> result1 = executeInDifferentThread( () ->
        {
            try ( Session session = neo4j.driver().session();
                  Transaction tx = session.beginTransaction() )
            {
                // lock first node
                updateNodeId( tx, nodeId1, newNodeId1 ).consume();

                latch1.await();
                latch2.countDown();

                // lock second node
                updateNodeId( tx, nodeId2, newNodeId1 ).consume();

                tx.success();
            }
            return null;
        } );

        Future<Void> result2 = executeInDifferentThread( () ->
        {
            try ( Session session = neo4j.driver().session() )
            {
                session.writeTransaction( tx ->
                {
                    // lock second node
                    updateNodeId( tx, nodeId2, newNodeId2 ).consume();

                    latch1.countDown();
                    await( latch2 );

                    // lock first node
                    updateNodeId( tx, nodeId1, newNodeId2 ).consume();

                    createNodeWithId( nodeId3 );

                    return null;
                } );
            }
            return null;
        } );

        boolean firstResultFailed = false;
        try
        {
            // first future may:
            // 1) succeed, when it's tx was able to grab both locks and tx in other future was
            //    terminated because of a deadlock
            // 2) fail, when it's tx was terminated because of a deadlock
            assertNull( result1.get( 20, TimeUnit.SECONDS ) );
        }
        catch ( ExecutionException e )
        {
            firstResultFailed = true;
        }

        // second future can't fail because deadlocks are retried
        assertNull( result2.get( 20, TimeUnit.SECONDS ) );

        if ( firstResultFailed )
        {
            // tx with retries was successful and updated ids
            assertEquals( 0, countNodesWithId( newNodeId1 ) );
            assertEquals( 2, countNodesWithId( newNodeId2 ) );
        }
        else
        {
            // tx without retries was successful and updated ids
            // tx with retries did not manage to find nodes because their ids were updated
            assertEquals( 2, countNodesWithId( newNodeId1 ) );
            assertEquals( 0, countNodesWithId( newNodeId2 ) );
        }
        // tx with retries was successful and created an additional node
        assertEquals( 1, countNodesWithId( nodeId3 ) );
    }

    @Test
    void shouldExecuteTransactionWorkInCallerThread()
    {
        int maxFailures = 3;
        Thread callerThread = Thread.currentThread();

        try ( Session session = neo4j.driver().session() )
        {
            String result = session.readTransaction( new TransactionWork<String>()
            {
                int failures;

                @Override
                public String execute( Transaction tx )
                {
                    assertSame( callerThread, Thread.currentThread() );
                    if ( failures++ < maxFailures )
                    {
                        throw new ServiceUnavailableException( "Oh no" );
                    }
                    return "Hello";
                }
            } );

            assertEquals( "Hello", result );
        }
    }

    @Test
    void shouldPropagateRunFailureWhenClosed()
    {
        Session session = neo4j.driver().session();

        session.run( "RETURN 10 / 0" );

        ClientException e = assertThrows( ClientException.class, session::close );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
    }

    @Test
    void shouldPropagatePullAllFailureWhenClosed()
    {
        Session session = neo4j.driver().session();

        session.run( "UNWIND range(20000, 0, -1) AS x RETURN 10 / x" );

        ClientException e = assertThrows( ClientException.class, session::close );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
    }

    @Test
    void shouldBePossibleToConsumeResultAfterSessionIsClosed()
    {
        StatementResult result;
        try ( Session session = neo4j.driver().session() )
        {
            result = session.run( "UNWIND range(1, 20000) AS x RETURN x" );
        }

        List<Integer> ints = result.list( record -> record.get( 0 ).asInt() );
        assertEquals( 20000, ints.size() );
    }

    @Test
    void shouldPropagateFailureFromSummary()
    {
        try ( Session session = neo4j.driver().session() )
        {
            StatementResult result = session.run( "RETURN Wrong" );

            ClientException e = assertThrows( ClientException.class, result::summary );
            assertThat( e.code(), containsString( "SyntaxError" ) );
            assertNotNull( result.summary() );
        }
    }

    @Test
    void shouldThrowFromCloseWhenPreviousErrorNotConsumed()
    {
        Session session = neo4j.driver().session();

        session.run( "CREATE ()" );
        session.run( "CREATE ()" );
        session.run( "RETURN 10 / 0" );

        ClientException e = assertThrows( ClientException.class, session::close );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
    }

    @Test
    void shouldThrowFromRunWhenPreviousErrorNotConsumed()
    {
        Session session = neo4j.driver().session();

        session.run( "CREATE ()" );
        session.run( "CREATE ()" );
        session.run( "RETURN 10 / 0" );

        try
        {
            ClientException e = assertThrows( ClientException.class, () -> session.run( "CREATE ()" ) );
            assertThat( e.getMessage(), containsString( "/ by zero" ) );
        }
        finally
        {
            session.close();
        }
    }

    @Test
    void shouldCloseCleanlyWhenRunErrorConsumed()
    {
        Session session = neo4j.driver().session();

        session.run( "CREATE ()" );

        ClientException e = assertThrows( ClientException.class, () -> session.run( "RETURN 10 / 0" ).consume() );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );

        session.run( "CREATE ()" );

        session.close();
        assertFalse( session.isOpen() );
    }

    @Test
    void shouldConsumePreviousResultBeforeRunningNewQuery()
    {
        try ( Session session = neo4j.driver().session() )
        {
            session.run( "UNWIND range(1000, 0, -1) AS x RETURN 42 / x" );

            ClientException e = assertThrows( ClientException.class, () -> session.run( "RETURN 1" ) );
            assertThat( e.getMessage(), containsString( "/ by zero" ) );
        }
    }

    @Test
    void shouldNotRetryOnConnectionAcquisitionTimeout()
    {
        int maxPoolSize = 3;
        Config config = Config.builder()
                .withMaxConnectionPoolSize( maxPoolSize )
                .withConnectionAcquisitionTimeout( 0, TimeUnit.SECONDS )
                .withMaxTransactionRetryTime( 42, TimeUnit.DAYS ) // retry for a really long time
                .build();

        driver = new DriverFactoryWithOneEventLoopThread().newInstance( neo4j.uri(), neo4j.authToken(), config );

        for ( int i = 0; i < maxPoolSize; i++ )
        {
            driver.session().beginTransaction();
        }

        AtomicInteger invocations = new AtomicInteger();
        ClientException e = assertThrows( ClientException.class,
                () -> driver.session().writeTransaction( tx -> invocations.incrementAndGet() ) );
        assertThat( e, is( connectionAcquisitionTimeoutError( 0 ) ) );

        // work should never be invoked
        assertEquals( 0, invocations.get() );
    }

    @Test
    void shouldAllowConsumingRecordsAfterFailureInSessionClose()
    {
        Session session = neo4j.driver().session();

        StatementResult result = session.run( "UNWIND [2, 4, 8, 0] AS x RETURN 32 / x" );

        ClientException e = assertThrows( ClientException.class, session::close );
        assertThat( e, is( arithmeticError() ) );

        assertTrue( result.hasNext() );
        assertEquals( 16, result.next().get( 0 ).asInt() );
        assertTrue( result.hasNext() );
        assertEquals( 8, result.next().get( 0 ).asInt() );
        assertTrue( result.hasNext() );
        assertEquals( 4, result.next().get( 0 ).asInt() );
        assertFalse( result.hasNext() );
    }

    @Test
    void shouldAllowAccessingRecordsAfterSummary()
    {
        int recordCount = 10_000;
        String query = "UNWIND range(1, " + recordCount + ") AS x RETURN x";

        try ( Session session = neo4j.driver().session() )
        {
            StatementResult result = session.run( query );

            ResultSummary summary = result.summary();
            assertEquals( query, summary.statement().text() );
            assertEquals( StatementType.READ_ONLY, summary.statementType() );

            List<Record> records = result.list();
            assertEquals( recordCount, records.size() );
            for ( int i = 1; i <= recordCount; i++ )
            {
                Record record = records.get( i - 1 );
                assertEquals( i, record.get( 0 ).asInt() );
            }
        }
    }

    @Test
    void shouldAllowAccessingRecordsAfterSessionClosed()
    {
        int recordCount = 11_333;
        String query = "UNWIND range(1, " + recordCount + ") AS x RETURN 'Result-' + x";

        StatementResult result;
        try ( Session session = neo4j.driver().session() )
        {
            result = session.run( query );
        }

        List<Record> records = result.list();
        assertEquals( recordCount, records.size() );
        for ( int i = 1; i <= recordCount; i++ )
        {
            Record record = records.get( i - 1 );
            assertEquals( "Result-" + i, record.get( 0 ).asString() );
        }
    }

    @Test
    void shouldAllowToConsumeRecordsSlowlyAndCloseSession() throws InterruptedException
    {
        Session session = neo4j.driver().session();

        StatementResult result = session.run( "UNWIND range(10000, 0, -1) AS x RETURN 10 / x" );

        // consume couple records slowly with a sleep in-between
        for ( int i = 0; i < 10; i++ )
        {
            assertTrue( result.hasNext() );
            assertNotNull( result.next() );
            Thread.sleep( 50 );
        }

        ClientException e = assertThrows( ClientException.class, session::close );
        assertThat( e, is( arithmeticError() ) );
    }

    @Test
    void shouldAllowToConsumeRecordsSlowlyAndRetrieveSummary() throws InterruptedException
    {
        try ( Session session = neo4j.driver().session() )
        {
            StatementResult result = session.run( "UNWIND range(8000, 1, -1) AS x RETURN 42 / x" );

            // consume couple records slowly with a sleep in-between
            for ( int i = 0; i < 12; i++ )
            {
                assertTrue( result.hasNext() );
                assertNotNull( result.next() );
                Thread.sleep( 50 );
            }

            ResultSummary summary = result.summary();
            assertNotNull( summary );
        }
    }

    @Test
    void shouldBeResponsiveToThreadInterruptWhenWaitingForResult()
    {
        try ( Session session1 = neo4j.driver().session();
              Session session2 = neo4j.driver().session() )
        {
            session1.run( "CREATE (:Person {name: 'Beta Ray Bill'})" ).consume();

            Transaction tx = session1.beginTransaction();
            tx.run( "MATCH (n:Person {name: 'Beta Ray Bill'}) SET n.hammer = 'Mjolnir'" ).consume();

            // now 'Beta Ray Bill' node is locked

            // setup other thread to interrupt current thread when it blocks
            TestUtil.interruptWhenInWaitingState( Thread.currentThread() );

            try
            {
                ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class,
                        () -> session2.run( "MATCH (n:Person {name: 'Beta Ray Bill'}) SET n.hammer = 'Stormbreaker'" ).consume() );
                assertThat( e.getMessage(), containsString( "Connection to the database terminated" ) );
                assertThat( e.getMessage(), containsString( "Thread interrupted" ) );
            }
            finally
            {
                // clear interrupted flag
                Thread.interrupted();
            }
        }
    }

    @Test
    void shouldAllowLongRunningQueryWithConnectTimeout() throws Exception
    {
        int connectionTimeoutMs = 3_000;
        Config config = Config.builder()
                .withLogging( DEV_NULL_LOGGING )
                .withConnectionTimeout( connectionTimeoutMs, TimeUnit.MILLISECONDS )
                .build();

        try ( Driver driver = GraphDatabase.driver( neo4j.uri(), neo4j.authToken(), config ) )
        {
            Session session1 = driver.session();
            Session session2 = driver.session();

            session1.run( "CREATE (:Avenger {name: 'Hulk'})" ).consume();

            Transaction tx = session1.beginTransaction();
            tx.run( "MATCH (a:Avenger {name: 'Hulk'}) SET a.power = 100 RETURN a" ).consume();

            // Hulk node is now locked

            CountDownLatch latch = new CountDownLatch( 1 );
            Future<Long> updateFuture = executeInDifferentThread( () ->
            {
                latch.countDown();
                return session2.run( "MATCH (a:Avenger {name: 'Hulk'}) SET a.weight = 1000 RETURN a.power" )
                        .single().get( 0 ).asLong();
            } );

            latch.await();
            // sleep more than connection timeout
            Thread.sleep( connectionTimeoutMs + 1_000 );
            // verify that query is still executing and has not failed because of the read timeout
            assertFalse( updateFuture.isDone() );

            tx.success();
            tx.close();

            long hulkPower = updateFuture.get( 10, TimeUnit.SECONDS );
            assertEquals( 100, hulkPower );
        }
    }

    @Test
    void shouldAllowReturningNullFromTransactionFunction()
    {
        try ( Session session = neo4j.driver().session() )
        {
            assertNull( session.readTransaction( tx -> null ) );
            assertNull( session.writeTransaction( tx -> null ) );
        }
    }

    @Test
    void shouldAllowIteratingOverEmptyResult()
    {
        try ( Session session = neo4j.driver().session() )
        {
            StatementResult result = session.run( "UNWIND [] AS x RETURN x" );
            assertFalse( result.hasNext() );

            assertThrows( NoSuchElementException.class, result::next );
        }
    }

    @Test
    void shouldAllowConsumingEmptyResult()
    {
        try ( Session session = neo4j.driver().session() )
        {
            StatementResult result = session.run( "UNWIND [] AS x RETURN x" );
            ResultSummary summary = result.consume();
            assertNotNull( summary );
            assertEquals( StatementType.READ_ONLY, summary.statementType() );
        }
    }

    @Test
    void shouldAllowListEmptyResult()
    {
        try ( Session session = neo4j.driver().session() )
        {
            StatementResult result = session.run( "UNWIND [] AS x RETURN x" );
            assertEquals( emptyList(), result.list() );
        }
    }

    @Test
    void shouldConsume()
    {
        try ( Session session = neo4j.driver().session() )
        {
            String query = "UNWIND [1, 2, 3, 4, 5] AS x RETURN x";
            StatementResult result = session.run( query );

            ResultSummary summary = result.consume();
            assertEquals( query, summary.statement().text() );
            assertEquals( StatementType.READ_ONLY, summary.statementType() );

            assertFalse( result.hasNext() );
            assertEquals( emptyList(), result.list() );
        }
    }

    @Test
    void shouldConsumeWithFailure()
    {
        try ( Session session = neo4j.driver().session() )
        {
            String query = "UNWIND [1, 2, 3, 4, 0] AS x RETURN 10 / x";
            StatementResult result = session.run( query );

            ClientException e = assertThrows( ClientException.class, result::consume );
            assertThat( e, is( arithmeticError() ) );

            assertFalse( result.hasNext() );
            assertEquals( emptyList(), result.list() );

            ResultSummary summary = result.summary();
            assertEquals( query, summary.statement().text() );
        }
    }

    @Test
    void shouldNotAllowStartingMultipleTransactions()
    {
        try ( Session session = neo4j.driver().session() )
        {
            Transaction tx = session.beginTransaction();
            assertNotNull( tx );

            for ( int i = 0; i < 3; i++ )
            {
                ClientException e = assertThrows( ClientException.class, session::beginTransaction );
                assertThat( e.getMessage(),
                        containsString( "You cannot begin a transaction on a session with an open transaction" ) );
            }

            tx.close();

            assertNotNull( session.beginTransaction() );
        }
    }

    @Test
    void shouldCloseOpenTransactionWhenClosed()
    {
        try ( Session session = neo4j.driver().session() )
        {
            Transaction tx = session.beginTransaction();
            tx.run( "CREATE (:Node {id: 123})" );
            tx.run( "CREATE (:Node {id: 456})" );

            tx.success();
        }

        assertEquals( 1, countNodesWithId( 123 ) );
        assertEquals( 1, countNodesWithId( 456 ) );
    }

    @Test
    void shouldRollbackOpenTransactionWhenClosed()
    {
        try ( Session session = neo4j.driver().session() )
        {
            Transaction tx = session.beginTransaction();
            tx.run( "CREATE (:Node {id: 123})" );
            tx.run( "CREATE (:Node {id: 456})" );

            tx.failure();
        }

        assertEquals( 0, countNodesWithId( 123 ) );
        assertEquals( 0, countNodesWithId( 456 ) );
    }

    @Test
    void shouldSupportNestedQueries()
    {
        try ( Session session = neo4j.driver().session() )
        {
            // populate db with test data
            session.run( "UNWIND range(1, 100) AS x CREATE (:Property {id: x})" ).consume();
            session.run( "UNWIND range(1, 10) AS x CREATE (:Resource {id: x})" ).consume();

            int seenProperties = 0;
            int seenResources = 0;

            // read properties and resources using a single session
            StatementResult properties = session.run( "MATCH (p:Property) RETURN p" );
            while ( properties.hasNext() )
            {
                assertNotNull( properties.next() );
                seenProperties++;

                StatementResult resources = session.run( "MATCH (r:Resource) RETURN r" );
                while ( resources.hasNext() )
                {
                    assertNotNull( resources.next() );
                    seenResources++;
                }
            }

            assertEquals( 100, seenProperties );
            assertEquals( 1000, seenResources );
        }
    }

    private void testExecuteReadTx( AccessMode sessionMode )
    {
        Driver driver = neo4j.driver();

        // write some test data
        try ( Session session = driver.session() )
        {
            session.run( "CREATE (:Person {name: 'Tony Stark'})" );
            session.run( "CREATE (:Person {name: 'Steve Rogers'})" );
        }

        // read previously committed data
        try ( Session session = driver.session( sessionMode ) )
        {
            Set<String> names = session.readTransaction( tx ->
            {
                List<Record> records = tx.run( "MATCH (p:Person) RETURN p.name AS name" ).list();
                Set<String> names1 = new HashSet<>( records.size() );
                for ( Record record : records )
                {
                    names1.add( record.get( "name" ).asString() );
                }
                return names1;
            } );

            assertThat( names, containsInAnyOrder( "Tony Stark", "Steve Rogers" ) );
        }
    }

    private void testExecuteWriteTx( AccessMode sessionMode )
    {
        Driver driver = neo4j.driver();

        // write some test data
        try ( Session session = driver.session( sessionMode ) )
        {
            String material = session.writeTransaction( tx ->
            {
                StatementResult result = tx.run( "CREATE (s:Shield {material: 'Vibranium'}) RETURN s" );
                tx.success();
                Record record = result.single();
                return record.get( 0 ).asNode().get( "material" ).asString();
            } );

            assertEquals( "Vibranium", material );
        }

        // read previously committed data
        try ( Session session = driver.session() )
        {
            Record record = session.run( "MATCH (s:Shield) RETURN s.material" ).single();
            assertEquals( "Vibranium", record.get( 0 ).asString() );
        }
    }

    private void testTxRollbackWhenFunctionThrows( AccessMode sessionMode )
    {
        Driver driver = neo4j.driver();

        try ( Session session = driver.session( sessionMode ) )
        {
            assertThrows( ClientException.class, () ->
                    session.writeTransaction( tx ->
                    {
                        tx.run( "CREATE (:Person {name: 'Thanos'})" );
                        // trigger division by zero error:
                        tx.run( "UNWIND range(0, 1) AS i RETURN 10/i" );
                        tx.success();
                        return null;
                    } ) );
        }

        // no data should have been committed
        try ( Session session = driver.session() )
        {
            Record record = session.run( "MATCH (p:Person {name: 'Thanos'}) RETURN count(p)" ).single();
            assertEquals( 0, record.get( 0 ).asInt() );
        }

    }

    private Driver newDriverWithoutRetries()
    {
        return newDriverWithFixedRetries( 0 );
    }

    private Driver newDriverWithFixedRetries( int maxRetriesCount )
    {
        DriverFactory driverFactory = new DriverFactoryWithFixedRetryLogic( maxRetriesCount );
        RoutingSettings routingConf = new RoutingSettings( 1, 1, RoutingContext.EMPTY );
        AuthToken auth = DEFAULT_AUTH_TOKEN;
        return driverFactory.newInstance( neo4j.uri(), auth, routingConf, RetrySettings.DEFAULT, noLoggingConfig() );
    }

    private Driver newDriverWithLimitedRetries( int maxTxRetryTime, TimeUnit unit )
    {
        Config config = Config.builder()
                .withLogging( DEV_NULL_LOGGING )
                .withMaxTransactionRetryTime( maxTxRetryTime, unit )
                .build();
        return GraphDatabase.driver( neo4j.uri(), neo4j.authToken(), config );
    }

    private static Config noLoggingConfig()
    {
        return Config.builder().withLogging( DEV_NULL_LOGGING ).build();
    }

    private static ThrowingWork newThrowingWorkSpy( String query, int failures )
    {
        return spy( new ThrowingWork( query, failures ) );
    }

    private int countNodesWithId( int id )
    {
        try ( Session session = neo4j.driver().session() )
        {
            StatementResult result = session.run( "MATCH (n {id: {id}}) RETURN count(n)", parameters( "id", id ) );
            return result.single().get( 0 ).asInt();
        }
    }

    private void createNodeWithId( int id )
    {
        try ( Session session = neo4j.driver().session() )
        {
            session.run( "CREATE (n {id: {id}})", parameters( "id", id ) );
        }
    }

    private static StatementResult updateNodeId( StatementRunner statementRunner, int currentId, int newId )
    {
        return statementRunner.run( "MATCH (n {id: {currentId}}) SET n.id = {newId}",
                parameters( "currentId", currentId, "newId", newId ) );
    }

    private static boolean assertOneOfTwoFuturesFailWithDeadlock( Future<Void> future1, Future<Void> future2 )
            throws Exception
    {
        boolean firstFailed = false;
        try
        {
            assertNull( future1.get( 20, TimeUnit.SECONDS ) );
        }
        catch ( ExecutionException e )
        {
            assertDeadlockDetectedError( e );
            firstFailed = true;
        }

        try
        {
            assertNull( future2.get( 20, TimeUnit.SECONDS ) );
        }
        catch ( ExecutionException e )
        {
            assertFalse( firstFailed, "Both futures failed" );
            assertDeadlockDetectedError( e );
        }
        return firstFailed;
    }

    private static void assertDeadlockDetectedError( ExecutionException e )
    {
        assertThat( e.getCause(), instanceOf( TransientException.class ) );
        String errorCode = ((TransientException) e.getCause()).code();
        assertEquals( "Neo.TransientError.Transaction.DeadlockDetected", errorCode );
    }

    private <T> Future<T> executeInDifferentThread( Callable<T> callable )
    {
        if ( executor == null )
        {
            executor = Executors.newCachedThreadPool( daemon( getClass().getSimpleName() + "-thread-" ) );
        }
        return executor.submit( callable );
    }

    private static void await( CountDownLatch latch )
    {
        try
        {
            latch.await();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException( e );
        }
    }

    private static class ThrowingWork implements TransactionWork<Record>
    {
        final String query;
        final int failures;

        int invoked;

        ThrowingWork( String query, int failures )
        {
            this.query = query;
            this.failures = failures;
        }

        @Override
        public Record execute( Transaction tx )
        {
            StatementResult result = tx.run( query );
            if ( invoked++ < failures )
            {
                throw new ServiceUnavailableException( "" );
            }
            tx.success();
            return result.single();
        }
    }
}
