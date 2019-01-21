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
package org.neo4j.docs.driver;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.neo4j.driver.Session;
import org.neo4j.driver.StatementResultCursor;
import org.neo4j.driver.Transaction;

public class AsyncExplicitTransactionExample extends BaseApplication
{
    public AsyncExplicitTransactionExample( String uri, String user, String password )
    {
        super( uri, user, password );
    }

    public CompletionStage<Void> printSingleProduct()
    {
        // tag::async-explicit-transaction[]
        String query = "MATCH (p:Product) WHERE p.id = $id RETURN p.title";
        Map<String,Object> parameters = Collections.singletonMap( "id", 0 );

        Session session = driver.session();

        Function<Transaction,CompletionStage<Void>> printSingleTitle = tx ->
                tx.runAsync( query, parameters )
                        .thenCompose( StatementResultCursor::singleAsync )
                        .thenApply( record -> record.get( 0 ).asString() )
                        .thenApply( title ->
                        {
                            // single title fetched successfully
                            System.out.println( title );
                            return true; // signal to commit the transaction
                        } )
                        .exceptionally( error ->
                        {
                            // query execution failed
                            error.printStackTrace();
                            return false; // signal to rollback the transaction
                        } )
                        .thenCompose( commit -> commit ? tx.commitAsync() : tx.rollbackAsync() );

        return session.beginTransactionAsync()
                .thenCompose( printSingleTitle )
                .exceptionally( error ->
                {
                    // either commit or rollback failed
                    error.printStackTrace();
                    return null;
                } )
                .thenCompose( ignore -> session.closeAsync() );
        // end::async-explicit-transaction[]
    }
}
