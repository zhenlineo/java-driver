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

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.internal.BookmarksHolder;
import org.neo4j.driver.internal.async.DecoratedConnection;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.Record;
import org.neo4j.driver.Statement;
import org.neo4j.driver.async.StatementResultCursor;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.net.ServerAddress;

import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;
import static org.neo4j.driver.internal.util.ServerVersion.v3_2_0;
import static org.neo4j.driver.Values.parameters;

public class RoutingProcedureRunner
{
    static final String GET_SERVERS = "dbms.cluster.routing.getServers";
    static final String GET_ROUTING_TABLE_PARAM = "context";
    static final String GET_ROUTING_TABLE = "dbms.cluster.routing.getRoutingTable({" + GET_ROUTING_TABLE_PARAM + "})";

    private final RoutingContext context;

    public RoutingProcedureRunner( RoutingContext context )
    {
        this.context = context;
    }

    public CompletionStage<RoutingProcedureResponse> run( CompletionStage<Connection> connectionStage )
    {
        return connectionStage.thenCompose( connection ->
        {
            // Routing procedure will be called on the default database
            DecoratedConnection delegate = new DecoratedConnection( connection, ABSENT_DB_NAME, AccessMode.WRITE );
            ServerAddress router = connection.serverAddress();
            Statement procedure = procedureStatement( delegate.serverVersion() );
            return runProcedure( delegate, procedure )
                    .thenCompose( records -> releaseConnection( delegate, records ) )
                    .handle( ( records, error ) -> processProcedureResponse( procedure, router, records, error ) );
        } );
    }

    CompletionStage<List<Record>> runProcedure( Connection connection, Statement procedure )
    {
        return connection.protocol()
                .runInAutoCommitTransaction( connection, procedure, BookmarksHolder.NO_OP, TransactionConfig.empty(), true )
                .asyncResult().thenCompose( StatementResultCursor::listAsync );
    }

    private Statement procedureStatement( ServerVersion serverVersion )
    {
        if ( serverVersion.greaterThanOrEqual( v3_2_0 ) )
        {
            return new Statement( "CALL " + GET_ROUTING_TABLE,
                    parameters( GET_ROUTING_TABLE_PARAM, context.asMap() ) );
        }
        else
        {
            return new Statement( "CALL " + GET_SERVERS );
        }
    }

    private CompletionStage<List<Record>> releaseConnection( Connection connection, List<Record> records )
    {
        // It is not strictly required to release connection after routing procedure invocation because it'll
        // be released by the PULL_ALL response handler after result is fully fetched. Such release will happen
        // in background. However, releasing it early as part of whole chain makes it easier to reason about
        // rediscovery in stub server tests. Some of them assume connections to instances not present in new
        // routing table will be closed immediately.
        return connection.release().thenApply( ignore -> records );
    }

    private RoutingProcedureResponse processProcedureResponse( Statement procedure, ServerAddress router, List<Record> records,
            Throwable error )
    {
        Throwable cause = Futures.completionExceptionCause( error );
        if ( cause != null )
        {
            return handleError( procedure, router, cause );
        }
        else
        {
            return new RoutingProcedureResponse( procedure, router, records );
        }
    }

    private RoutingProcedureResponse handleError( Statement procedure, ServerAddress router, Throwable error )
    {
        if ( error instanceof ClientException )
        {
            return new RoutingProcedureResponse( procedure, router, error );
        }
        else
        {
            throw new CompletionException( error );
        }
    }
}
