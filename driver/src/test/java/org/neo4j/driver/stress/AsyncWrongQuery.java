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
package org.neo4j.driver.stress;

import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.StatementResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class AsyncWrongQuery<C extends AbstractContext> extends AbstractAsyncQuery<C>
{
    public AsyncWrongQuery( Driver driver )
    {
        super( driver, false );
    }

    @Override
    public CompletionStage<Void> execute( C context )
    {
        Session session = newSession( AccessMode.READ, context );

        return session.runAsync( "RETURN Wrong" )
                .thenCompose( StatementResultCursor::nextAsync )
                .handle( ( record, error ) ->
                {
                    session.closeAsync();
                    assertNull( record );

                    Throwable cause = Futures.completionExceptionCause( error );
                    assertNotNull( cause );
                    assertThat( cause, instanceOf( ClientException.class ) );
                    assertThat( ((Neo4jException) cause).code(), containsString( "SyntaxError" ) );

                    return null;
                } );
    }
}
