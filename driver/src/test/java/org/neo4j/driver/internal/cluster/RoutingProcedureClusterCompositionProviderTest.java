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
package org.neo4j.driver.internal.cluster;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.Record;
import org.neo4j.driver.Statement;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ProtocolException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;

import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.util.TestUtil.await;

class RoutingProcedureClusterCompositionProviderTest
{
    @Test
    void shouldProtocolErrorWhenNoRecord()
    {
        // Given
        RoutingProcedureRunner mockedRunner = newProcedureRunnerMock();
        ClusterCompositionProvider provider =
                new RoutingProcedureClusterCompositionProvider( mock( Clock.class ), mockedRunner );

        CompletionStage<Connection> connectionStage = completedFuture( mock( Connection.class ) );
        RoutingProcedureResponse noRecordsResponse = newRoutingResponse();
        when( mockedRunner.run( connectionStage ) ).thenReturn( completedFuture( noRecordsResponse ) );

        // When
        ClusterCompositionResponse response = await( provider.getClusterComposition( connectionStage ) );

        // Then
        assertThat( response, instanceOf( ClusterCompositionResponse.Failure.class ) );
        ProtocolException error = assertThrows( ProtocolException.class, response::clusterComposition );
        assertThat( error.getMessage(), containsString( "records received '0' is too few or too many." ) );
    }

    @Test
    void shouldProtocolErrorWhenMoreThanOneRecord()
    {
        // Given
        RoutingProcedureRunner mockedRunner = newProcedureRunnerMock();
        ClusterCompositionProvider provider =
                new RoutingProcedureClusterCompositionProvider( mock( Clock.class ), mockedRunner );

        CompletionStage<Connection> connectionStage = completedFuture( mock( Connection.class ) );
        Record aRecord = new InternalRecord( asList( "key1", "key2" ), new Value[]{ new StringValue( "a value" ) } );
        RoutingProcedureResponse routingResponse = newRoutingResponse( aRecord, aRecord );
        when( mockedRunner.run( connectionStage ) ).thenReturn( completedFuture( routingResponse ) );

        // When
        ClusterCompositionResponse response = await( provider.getClusterComposition( connectionStage ) );

        // Then
        assertThat( response, instanceOf( ClusterCompositionResponse.Failure.class ) );
        ProtocolException error = assertThrows( ProtocolException.class, response::clusterComposition );
        assertThat( error.getMessage(), containsString( "records received '2' is too few or too many." ) );
    }

    @Test
    void shouldProtocolErrorWhenUnparsableRecord()
    {
        // Given
        RoutingProcedureRunner mockedRunner = newProcedureRunnerMock();
        ClusterCompositionProvider provider =
                new RoutingProcedureClusterCompositionProvider( mock( Clock.class ), mockedRunner );

        CompletionStage<Connection> connectionStage = completedFuture( mock( Connection.class ) );
        Record aRecord = new InternalRecord( asList( "key1", "key2" ), new Value[]{ new StringValue( "a value" ) } );
        RoutingProcedureResponse routingResponse = newRoutingResponse( aRecord );
        when( mockedRunner.run( connectionStage ) ).thenReturn( completedFuture( routingResponse ) );

        // When
        ClusterCompositionResponse response = await( provider.getClusterComposition( connectionStage ) );

        // Then
        assertThat( response, instanceOf( ClusterCompositionResponse.Failure.class ) );
        ProtocolException error = assertThrows( ProtocolException.class, response::clusterComposition );
        assertThat( error.getMessage(), containsString( "unparsable record received." ) );
    }

    @Test
    void shouldProtocolErrorWhenNoRouters()
    {
        // Given
        RoutingProcedureRunner mockedRunner = newProcedureRunnerMock();
        Clock mockedClock = mock( Clock.class );
        ClusterCompositionProvider provider =
                new RoutingProcedureClusterCompositionProvider( mockedClock, mockedRunner );

        CompletionStage<Connection> connectionStage = completedFuture( mock( Connection.class ) );
        Record record = new InternalRecord( asList( "ttl", "servers" ), new Value[]{
                value( 100 ), value( asList(
                serverInfo( "READ", "one:1337", "two:1337" ),
                serverInfo( "WRITE", "one:1337" ) ) )
        } );
        RoutingProcedureResponse routingResponse = newRoutingResponse( record );
        when( mockedRunner.run( connectionStage ) ).thenReturn( completedFuture( routingResponse ) );
        when( mockedClock.millis() ).thenReturn( 12345L );

        // When
        ClusterCompositionResponse response = await( provider.getClusterComposition( connectionStage ) );

        // Then
        assertThat( response, instanceOf( ClusterCompositionResponse.Failure.class ) );
        ProtocolException error = assertThrows( ProtocolException.class, response::clusterComposition );
        assertThat( error.getMessage(), containsString( "no router or reader found in response." ) );
    }

    @Test
    void shouldProtocolErrorWhenNoReaders()
    {
        // Given
        RoutingProcedureRunner mockedRunner = newProcedureRunnerMock();
        Clock mockedClock = mock( Clock.class );
        ClusterCompositionProvider provider =
                new RoutingProcedureClusterCompositionProvider( mockedClock, mockedRunner );

        CompletionStage<Connection> connectionStage = completedFuture( mock( Connection.class ) );
        Record record = new InternalRecord( asList( "ttl", "servers" ), new Value[]{
                value( 100 ), value( asList(
                serverInfo( "WRITE", "one:1337" ),
                serverInfo( "ROUTE", "one:1337", "two:1337" ) ) )
        } );
        RoutingProcedureResponse routingResponse = newRoutingResponse( record );
        when( mockedRunner.run( connectionStage ) ).thenReturn( completedFuture( routingResponse ) );
        when( mockedClock.millis() ).thenReturn( 12345L );

        // When
        ClusterCompositionResponse response = await( provider.getClusterComposition( connectionStage ) );

        // Then
        assertThat( response, instanceOf( ClusterCompositionResponse.Failure.class ) );
        ProtocolException error = assertThrows( ProtocolException.class, response::clusterComposition );
        assertThat( error.getMessage(), containsString( "no router or reader found in response." ) );
    }


    @Test
    void shouldPropagateConnectionFailureExceptions()
    {
        // Given
        RoutingProcedureRunner mockedRunner = newProcedureRunnerMock();
        ClusterCompositionProvider provider =
                new RoutingProcedureClusterCompositionProvider( mock( Clock.class ), mockedRunner );

        CompletionStage<Connection> connectionStage = completedFuture( mock( Connection.class ) );
        when( mockedRunner.run( connectionStage ) ).thenReturn( failedFuture(
                new ServiceUnavailableException( "Connection breaks during cypher execution" ) ) );

        // When & Then
        ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class, () -> await( provider.getClusterComposition( connectionStage ) ) );
        assertThat( e.getMessage(), containsString( "Connection breaks during cypher execution" ) );
    }

    @Test
    void shouldReturnSuccessResultWhenNoError()
    {
        // Given
        Clock mockedClock = mock( Clock.class );
        RoutingProcedureRunner mockedRunner = newProcedureRunnerMock();
        ClusterCompositionProvider provider =
                new RoutingProcedureClusterCompositionProvider( mockedClock, mockedRunner );

        CompletionStage<Connection> connectionStage = completedFuture( mock( Connection.class ) );
        Record record = new InternalRecord( asList( "ttl", "servers" ), new Value[]{
                value( 100 ), value( asList(
                serverInfo( "READ", "one:1337", "two:1337" ),
                serverInfo( "WRITE", "one:1337" ),
                serverInfo( "ROUTE", "one:1337", "two:1337" ) ) )
        } );
        RoutingProcedureResponse routingResponse = newRoutingResponse( record );
        when( mockedRunner.run( connectionStage ) ).thenReturn( completedFuture( routingResponse ) );
        when( mockedClock.millis() ).thenReturn( 12345L );

        // When
        ClusterCompositionResponse response = await( provider.getClusterComposition( connectionStage ) );

        // Then
        assertThat( response, instanceOf( ClusterCompositionResponse.Success.class ) );
        ClusterComposition cluster = response.clusterComposition();
        assertEquals( 12345 + 100_000, cluster.expirationTimestamp() );
        assertEquals( serverSet( "one:1337", "two:1337" ), cluster.readers() );
        assertEquals( serverSet( "one:1337" ), cluster.writers() );
        assertEquals( serverSet( "one:1337", "two:1337" ), cluster.routers() );
    }

    @Test
    @SuppressWarnings( "unchecked" )
    void shouldReturnFailureWhenProcedureRunnerFails()
    {
        RoutingProcedureRunner procedureRunner = newProcedureRunnerMock();
        RuntimeException error = new RuntimeException( "hi" );
        when( procedureRunner.run( any( CompletionStage.class ) ) )
                .thenReturn( completedFuture( newRoutingResponse( error ) ) );

        RoutingProcedureClusterCompositionProvider provider =
                new RoutingProcedureClusterCompositionProvider( mock( Clock.class ), procedureRunner );

        CompletionStage<Connection> connectionStage = completedFuture( mock( Connection.class ) );
        ClusterCompositionResponse response = await( provider.getClusterComposition( connectionStage ) );

        ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class, response::clusterComposition );
        assertEquals( error, e.getCause() );
    }

    private static Map<String,Object> serverInfo( String role, String... addresses )
    {
        Map<String,Object> map = new HashMap<>();
        map.put( "role", role );
        map.put( "addresses", asList( addresses ) );
        return map;
    }

    private static Set<BoltServerAddress> serverSet( String... addresses )
    {
        Set<BoltServerAddress> result = new HashSet<>();
        for ( String address : addresses )
        {
            result.add( new BoltServerAddress( address ) );
        }
        return result;
    }

    private static RoutingProcedureRunner newProcedureRunnerMock()
    {
        return mock( RoutingProcedureRunner.class );
    }

    private static RoutingProcedureResponse newRoutingResponse( Record... records )
    {
        return new RoutingProcedureResponse( new Statement( "procedure" ), asList( records ) );
    }

    private static RoutingProcedureResponse newRoutingResponse( Throwable error )
    {
        return new RoutingProcedureResponse( new Statement( "procedure" ), error );
    }
}
