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
package org.neo4j.driver.internal.handlers;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.Statement;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.exceptions.TransientException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.messaging.v1.BoltProtocolV1.METADATA_EXTRACTOR;

class TransactionPullAllResponseHandlerTest
{
    @Test
    void shouldMarkTransactionAsTerminatedOnFailures()
    {
        testErrorHandling( new ClientException( "Neo.ClientError.Cluster.NotALeader", "" ) );
        testErrorHandling( new ClientException( "Neo.ClientError.Procedure.ProcedureCallFailed", "" ) );
        testErrorHandling( new TransientException( "Neo.TransientError.Transaction.Terminated", "" ) );
        testErrorHandling( new TransientException( "Neo.TransientError.General.DatabaseUnavailable", "" ) );

        testErrorHandling( new RuntimeException() );
        testErrorHandling( new IOException() );
        testErrorHandling( new ServiceUnavailableException( "" ) );
        testErrorHandling( new SessionExpiredException( "" ) );
        testErrorHandling( new SessionExpiredException( "" ) );
        testErrorHandling( new ClientException( "Neo.ClientError.Request.Invalid" ) );
    }

    private static void testErrorHandling( Throwable error )
    {
        Connection connection = mock( Connection.class );
        when( connection.serverAddress() ).thenReturn( BoltServerAddress.LOCAL_DEFAULT );
        when( connection.serverVersion() ).thenReturn( ServerVersion.v3_2_0 );
        ExplicitTransaction tx = mock( ExplicitTransaction.class );
        RunResponseHandler runHandler = new RunResponseHandler( new CompletableFuture<>(), METADATA_EXTRACTOR );
        PullAllResponseHandler handler = new TransactionPullAllResponseHandler( new Statement( "RETURN 1" ), runHandler,
                connection, tx, METADATA_EXTRACTOR );

        handler.onFailure( error );

        verify( tx ).markTerminated();
    }
}
