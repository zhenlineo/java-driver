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
package org.neo4j.driver.v1.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import org.neo4j.driver.react.InternalRxSession;
import org.neo4j.driver.react.RxResult;
import org.neo4j.driver.react.RxSession;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.util.DatabaseExtension;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RxSessionIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @Test
    void shouldStream() throws Throwable
    {
        // Give
        Driver driver = GraphDatabase.driver( neo4j.uri(), neo4j.authToken(), Config.build().withLogging( Logging.console( Level.FINE ) ).toConfig() );

        // When
        Session session = driver.session();
        StatementResult result = session.run( "UNWIND range(1, 100) as n RETURN n" );

//        while( result.hasNext() )
//        {
//            System.out.println( result.next() );
//        }
        result.list(record -> {
            System.out.println(record);
            return record;
        });
//        result.stream().forEach( r -> System.out.println( r ) );
        System.out.println( result.summary() );
        driver.close();
        // Then
    }

    @Test
    void should() throws Throwable
    {
        // Give
        Driver driver = GraphDatabase.driver( neo4j.uri(), neo4j.authToken(), Config.build().withLogging( Logging.console( Level.FINE ) ).toConfig() );

        // When
        Session session = driver.session();
        StatementResult result = session.run( "UNWIND [1, 2, 0, 4] as n RETURN 10/n" );

        result.stream().forEach( r -> System.out.println( r ) );
        driver.close();
        // Then
    }

    @Test
    void shouldReceiveAllRecords() throws Throwable
    {
        // Give
        RxSession session = neo4j.driver().rxSession();
        RxResult result = session.run( "UNWIND range(1, 1000) as n RETURN n" );

        AtomicInteger count = new AtomicInteger();
        Flux.from( result.records() ).limitRate( 300 ) // batch size
                .doOnNext( r -> count.getAndIncrement() ).then( Mono.from( result.summary() ) ).doOnSuccess(
                summary -> assertThat( summary, notNullValue() ) ).then( Mono.from( session.close() ) ).block();
    }

    @Test
    void shouldReceiveErrorInOrder() throws Throwable
    {
        RxSession session = new InternalRxSession( neo4j.driver().session() );
        RxResult result = session.run( "UNWIND [1, 2, 1, 2, 0, 1] as n RETURN 10/n" );
        Flux<Record> recordFlux = Flux.from( result.records() );
        AtomicInteger count = new AtomicInteger();
        assertThrows( ClientException.class, () -> {
            recordFlux.toStream( 2 ).forEach( record -> {
                count.getAndIncrement();
            } );
            recordFlux.blockLast();
        } );

        assertThat( count, equalTo( 4 ) );
    }
}
