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
package org.neo4j.driver.internal;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.net.URI;

import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.util.StubServer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.driver.v1.util.StubServer.INSECURE_CONFIG;

class ReactDriverBoltKitTest
{
    @Test
    void shouldReact() throws Exception
    {
        StubServer server = StubServer.start( "react.script", 9001 );
        URI uri = URI.create( "bolt://127.0.0.1:9001" );
        int x;

        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG ) )
        {
            try ( Session session = driver.session() )
            {
                Futures.getNow( session.runAsync( "unwind range(1, 15) as n RETURN n" ).handle( (result, error) -> {

                    if ( result != null )
                    {
                        Publisher<Record> publisher = result.publisher();
                        publisher.subscribe( new Subscriber<Record>()
                        {
                            Subscription subscription;
                            final long bufferSize = 10;
                            long count;

                            @Override
                            public void onSubscribe( Subscription s )
                            {
                                count = bufferSize; // re-request when half consumed
//                                (subscription = s).request( bufferSize );
                            }

                            @Override
                            public void onNext( Record record )
                            {
                                if ( --count <= 0 )
                                {
                                    subscription.request(bufferSize);
                                    count += bufferSize;
                                }
                                System.out.println( record.toString() );
                            }

                            @Override
                            public void onError( Throwable t )
                            {
                                t.printStackTrace();
                            }

                            @Override
                            public void onComplete()
                            {
                                result.summaryAsync().thenApply( r -> {
                                    System.out.println( r );
                                    return Futures.completedWithNull();
                                } );
                            }
                        } );
                    }
                    return Futures.completedWithNull();
                } )  );
            }
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }
}
