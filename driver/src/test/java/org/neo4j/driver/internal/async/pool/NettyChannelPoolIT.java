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
package org.neo4j.driver.internal.async.pool;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.async.BootstrapFactory;
import org.neo4j.driver.internal.async.ChannelConnectorImpl;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.internal.util.ImmediateSchedulingEventExecutor;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.Neo4jRunner;
import org.neo4j.driver.util.ParallelizableIT;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.metrics.InternalAbstractMetrics.DEV_NULL_METRICS;
import static org.neo4j.driver.Values.value;

@ParallelizableIT
class NettyChannelPoolIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private Bootstrap bootstrap;
    private NettyChannelTracker poolHandler;
    private NettyChannelPool pool;

    @BeforeEach
    void setUp()
    {
        bootstrap = BootstrapFactory.newBootstrap( 1 );
        poolHandler = mock( NettyChannelTracker.class );
    }

    @AfterEach
    void tearDown()
    {
        if ( pool != null )
        {
            pool.close();
        }
        if ( bootstrap != null )
        {
            bootstrap.config().group().shutdownGracefully().syncUninterruptibly();
        }
    }

    @Test
    void shouldAcquireAndReleaseWithCorrectCredentials() throws Exception
    {
        pool = newPool( neo4j.authToken() );

        Future<Channel> acquireFuture = pool.acquire();
        acquireFuture.await( 5, TimeUnit.SECONDS );

        assertTrue( acquireFuture.isSuccess() );
        Channel channel = acquireFuture.getNow();
        assertNotNull( channel );
        verify( poolHandler ).channelCreated( eq( channel ), any() );
        verify( poolHandler, never() ).channelReleased( channel );

        Future<Void> releaseFuture = pool.release( channel );
        releaseFuture.await( 5, TimeUnit.SECONDS );

        assertTrue( releaseFuture.isSuccess() );
        verify( poolHandler ).channelReleased( channel );
    }

    @Test
    void shouldFailToAcquireWithWrongCredentials() throws Exception
    {
        pool = newPool( AuthTokens.basic( "wrong", "wrong" ) );

        Future<Channel> future = pool.acquire();
        future.await( 5, TimeUnit.DAYS );

        assertTrue( future.isDone() );
        assertNotNull( future.cause() );
        assertThat( future.cause(), instanceOf( AuthenticationException.class ) );

        verify( poolHandler, never() ).channelCreated( any() );
        verify( poolHandler, never() ).channelReleased( any() );
    }

    @Test
    void shouldAllowAcquireAfterFailures() throws Exception
    {
        int maxConnections = 2;

        Map<String,Value> authTokenMap = new HashMap<>();
        authTokenMap.put( "scheme", value( "basic" ) );
        authTokenMap.put( "principal", value( "neo4j" ) );
        authTokenMap.put( "credentials", value( "wrong" ) );
        InternalAuthToken authToken = new InternalAuthToken( authTokenMap );

        pool = newPool( authToken, maxConnections );

        for ( int i = 0; i < maxConnections; i++ )
        {
            ExecutionException e = assertThrows( ExecutionException.class, () -> acquire( pool ) );
            assertThat( e.getCause(), instanceOf( AuthenticationException.class ) );
        }

        authTokenMap.put( "credentials", value( Neo4jRunner.PASSWORD ) );

        assertNotNull( acquire( pool ) );
    }

    @Test
    void shouldLimitNumberOfConcurrentConnections() throws Exception
    {
        int maxConnections = 5;
        pool = newPool( neo4j.authToken(), maxConnections );

        for ( int i = 0; i < maxConnections; i++ )
        {
            assertNotNull( acquire( pool ) );
        }

        ExecutionException e = assertThrows( ExecutionException.class, () -> acquire( pool ) );
        assertThat( e.getCause(), instanceOf( TimeoutException.class ) );
        assertEquals( e.getCause().getMessage(), "Acquire operation took longer then configured maximum time" );
    }

    @Test
    void shouldTrackActiveChannels() throws Exception
    {
        NettyChannelTracker tracker = new NettyChannelTracker( DEV_NULL_METRICS, new ImmediateSchedulingEventExecutor(), DEV_NULL_LOGGING );

        poolHandler = tracker;
        pool = newPool( neo4j.authToken() );

        Channel channel1 = acquire( pool );
        Channel channel2 = acquire( pool );
        Channel channel3 = acquire( pool );
        assertEquals( 3, tracker.inUseChannelCount( neo4j.address() ) );

        release( channel1 );
        release( channel2 );
        release( channel3 );
        assertEquals( 0, tracker.inUseChannelCount( neo4j.address() ) );

        assertNotNull( acquire( pool ) );
        assertNotNull( acquire( pool ) );
        assertEquals( 2, tracker.inUseChannelCount( neo4j.address() ) );
    }

    private NettyChannelPool newPool( AuthToken authToken )
    {
        return newPool( authToken, 100 );
    }

    private NettyChannelPool newPool( AuthToken authToken, int maxConnections )
    {
        ConnectionSettings settings = new ConnectionSettings( authToken, 5_000 );
        ChannelConnectorImpl connector = new ChannelConnectorImpl( settings, SecurityPlan.insecure(), DEV_NULL_LOGGING,
                new FakeClock() );
        return new NettyChannelPool( neo4j.address(), connector, bootstrap, poolHandler, ChannelHealthChecker.ACTIVE,
                1_000, maxConnections );
    }

    private static Channel acquire( NettyChannelPool pool ) throws Exception
    {
        return pool.acquire().get( 5, TimeUnit.SECONDS );
    }

    private void release( Channel channel ) throws Exception
    {
        pool.release( channel ).get( 5, TimeUnit.SECONDS );
    }
}
