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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.neo4j.driver.Record;
import org.neo4j.driver.StatementResult;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.util.ParallelizableIT;
import org.neo4j.driver.util.SessionExtension;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Values.parameters;

@ParallelizableIT
class ResultStreamIT
{
    @RegisterExtension
    static final SessionExtension session = new SessionExtension();

    @Test
    void shouldAllowIteratingOverResultStream()
    {
        // When
        StatementResult res = session.run( "UNWIND [1,2,3,4] AS a RETURN a" );

        // Then I should be able to iterate over the result
        int idx = 1;
        while ( res.hasNext() )
        {
            assertEquals( idx++, res.next().get( "a" ).asLong() );
        }
    }

    @Test
    void shouldHaveFieldNamesInResult()
    {
        // When
        StatementResult res = session.run( "CREATE (n:TestNode {name:'test'}) RETURN n" );

        // Then
        assertEquals( "[n]", res.keys().toString() );
        assertNotNull( res.single() );
        assertEquals( "[n]", res.keys().toString() );
    }

    @Test
    void shouldGiveHelpfulFailureMessageWhenAccessNonExistingField()
    {
        // Given
        StatementResult rs =
                session.run( "CREATE (n:Person {name:{name}}) RETURN n", parameters( "name", "Tom Hanks" ) );

        // When
        Record single = rs.single();

        // Then
        assertTrue( single.get( "m" ).isNull() );
    }

    @Test
    void shouldGiveHelpfulFailureMessageWhenAccessNonExistingPropertyOnNode()
    {
        // Given
        StatementResult rs =
                session.run( "CREATE (n:Person {name:{name}}) RETURN n", parameters( "name", "Tom Hanks" ) );

        // When
        Record record = rs.single();

        // Then
        assertTrue( record.get( "n" ).get( "age" ).isNull() );
    }

    @Test
    void shouldNotReturnNullKeysOnEmptyResult()
    {
        // Given
        StatementResult rs = session.run( "CREATE (n:Person {name:{name}})", parameters( "name", "Tom Hanks" ) );

        // THEN
        assertNotNull( rs.keys() );
    }

    @Test
    void shouldBeAbleToReuseSessionAfterFailure()
    {
        // Given
        StatementResult res1 = session.run( "INVALID" );
        assertThrows( Exception.class, res1::consume );

        // When
        StatementResult res2 = session.run( "RETURN 1" );

        // Then
        assertTrue( res2.hasNext() );
        assertEquals( res2.next().get("1").asLong(), 1L );
    }

    @Test
    void shouldBeAbleToAccessSummaryAfterFailure()
    {
        // Given
        StatementResult res1 = session.run( "INVALID" );
        ResultSummary summary;

        // When
        try
        {
            res1.consume();
        }
        catch ( Exception e )
        {
            //ignore
        }
        finally
        {
            summary = res1.summary();
        }

        // Then
        assertThat( summary, notNullValue() );
        assertThat( summary.server().address(), equalTo( "localhost:" + session.boltPort() ) );
        assertThat( summary.counters().nodesCreated(), equalTo( 0 ) );
    }

    @Test
    void shouldBeAbleToAccessSummaryAfterTransactionFailure()
    {
        AtomicReference<StatementResult> resultRef = new AtomicReference<>();

        assertThrows( ClientException.class, () ->
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                StatementResult result = tx.run( "UNWIND [1,2,0] AS x RETURN 10/x" );
                resultRef.set( result );
                tx.success();
            }
        } );

        StatementResult result = resultRef.get();
        assertNotNull( result );
        assertEquals( 0, result.summary().counters().nodesCreated() );
    }

    @Test
    void shouldBufferRecordsAfterSummary()
    {
        // Given
        StatementResult result = session.run("UNWIND [1,2] AS a RETURN a");

        // When
        ResultSummary summary = result.summary();

        // Then
        assertThat( summary, notNullValue() );
        assertThat( summary.server().address(), equalTo( "localhost:" + session.boltPort() ) );
        assertThat( summary.counters().nodesCreated(), equalTo( 0 ) );

        assertThat( result.next().get( "a" ).asInt(), equalTo( 1 ) );
        assertThat( result.next().get( "a" ).asInt(), equalTo( 2 ) );
    }

    @Test
    void shouldDiscardRecordsAfterConsume()
    {
        // Given
        StatementResult result = session.run("UNWIND [1,2] AS a RETURN a");

        // When
        ResultSummary summary = result.consume();

        // Then
        assertThat( summary, notNullValue() );
        assertThat( summary.server().address(), equalTo( "localhost:" + session.boltPort() ) );
        assertThat( summary.counters().nodesCreated(), equalTo( 0 ) );

        assertThat( result.hasNext(), equalTo( false ) );
    }

    @Test
    void shouldHasNoElementsAfterFailure()
    {
        StatementResult result = session.run( "INVALID" );

        assertThrows( ClientException.class, result::hasNext );
        assertFalse( result.hasNext() );
    }

    @Test
    void shouldBeAnEmptyLitAfterFailure()
    {
        StatementResult result = session.run( "UNWIND (0, 1) as i RETURN 10 / i" );

        assertThrows( ClientException.class, result::list );
        assertTrue( result.list().isEmpty() );
    }

    @Test
    void shouldConvertEmptyStatementResultToStream()
    {
        long count = session.run( "MATCH (n:WrongLabel) RETURN n" )
                .stream()
                .count();

        assertEquals( 0, count );

        Optional<Record> anyRecord = session.run( "MATCH (n:OtherWrongLabel) RETURN n" )
                .stream()
                .findAny();

        assertFalse( anyRecord.isPresent() );
    }

    @Test
    void shouldConvertStatementResultToStream()
    {
        List<Integer> receivedList = session.run( "UNWIND range(1, 10) AS x RETURN x" )
                .stream()
                .map( record -> record.get( 0 ) )
                .map( Value::asInt )
                .collect( toList() );

        assertEquals( asList( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ), receivedList );
    }

    @Test
    void shouldConvertImmediatelyFailingStatementResultToStream()
    {
        List<Integer> seen = new ArrayList<>();

        ClientException e = assertThrows( ClientException.class,
                () -> session.run( "RETURN 10 / 0" )
                        .stream()
                        .forEach( record -> seen.add( record.get( 0 ).asInt() ) ) );

        assertThat( e.getMessage(), containsString( "/ by zero" ) );

        assertEquals( emptyList(), seen );
    }

    @Test
    void shouldConvertEventuallyFailingStatementResultToStream()
    {
        List<Integer> seen = new ArrayList<>();

        ClientException e = assertThrows( ClientException.class,
                () -> session.run( "UNWIND range(5, 0, -1) AS x RETURN x / x" )
                        .stream()
                        .forEach( record -> seen.add( record.get( 0 ).asInt() ) ) );

        assertThat( e.getMessage(), containsString( "/ by zero" ) );

        // stream should manage to consume all elements except the last one, which produces an error
        assertEquals( asList( 1, 1, 1, 1, 1 ), seen );
    }

    @Test
    void shouldEmptyResultWhenConvertedToStream()
    {
        StatementResult result = session.run( "UNWIND range(1, 10) AS x RETURN x" );

        assertTrue( result.hasNext() );
        assertEquals( 1, result.next().get( 0 ).asInt() );

        assertTrue( result.hasNext() );
        assertEquals( 2, result.next().get( 0 ).asInt() );

        List<Integer> list = result.stream()
                .map( record -> record.get( 0 ).asInt() )
                .collect( toList() );
        assertEquals( asList( 3, 4, 5, 6, 7, 8, 9, 10 ), list );

        assertFalse( result.hasNext() );
        assertThrows( NoSuchRecordException.class, result::next );
        assertEquals( emptyList(), result.list() );
        assertEquals( 0, result.stream().count() );
    }

    @Test
    void shouldConsumeLargeResultAsParallelStream()
    {
        List<String> receivedList = session.run( "UNWIND range(1, 200000) AS x RETURN 'value-' + x" )
                .stream()
                .parallel()
                .map( record -> record.get( 0 ) )
                .map( Value::asString )
                .collect( toList() );

        List<String> expectedList = IntStream.range( 1, 200001 )
                .mapToObj( i -> "value-" + i )
                .collect( toList() );

        assertEquals( expectedList, receivedList );
    }
}
