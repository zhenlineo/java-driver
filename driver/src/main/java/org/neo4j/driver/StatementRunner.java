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
package org.neo4j.driver;

import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.neo4j.driver.types.TypeSystem;
import org.neo4j.driver.util.Experimental;

/**
 * Common interface for components that can execute Neo4j statements.
 *
 * <h2>Important notes on semantics</h2>
 *
 * Statements run in the same {@link StatementRunner} are guaranteed
 * to execute in order, meaning changes made by one statement will be seen
 * by all subsequent statements in the same {@link StatementRunner}.
 *
 * However, to allow handling very large results, and to improve performance,
 * result streams are retrieved lazily from the network. This means that when
 * any of the blocking {@link #run(Statement)} or async {@link #runAsync(Statement)}
 * methods return a result, the statement has only started executing - it may not
 * have completed yet. Most of the time, you will not notice this, because the
 * driver automatically waits for statements to complete at specific points to
 * fulfill its contracts.
 *
 * Specifically, the driver will ensure all outstanding statements are completed
 * whenever you:
 *
 * <ul>
 *     <li>Read from or discard a result, for instance via blocking
 *     {@link StatementResult#next()}, {@link StatementResult#consume()} or async
 *     {@link StatementResultCursor#nextAsync()}, {@link StatementResultCursor#consumeAsync()}</li>
 *     <li>Explicitly commit/rollback a transaction using blocking {@link Transaction#close()}
 *     or async {@link Transaction#commitAsync()}, {@link Transaction#rollbackAsync()}</li>
 *     <li>Close a session using blocking {@link Session#close()} or
 *     async {@link Session#closeAsync()}</li>
 * </ul>
 *
 * As noted, most of the time, you will not need to consider this - your writes will
 * always be durably stored as long as you either use the results, explicitly commit
 * {@link Transaction transactions} or close the session you used using {@link Session#close()}.
 *
 * While these semantics introduce some complexity, it gives the driver the ability
 * to handle infinite result streams (like subscribing to events), significantly lowers
 * the memory overhead for your application and improves performance.
 *
 * <h2>Asynchronous API</h2>
 *
 * All overloads of {@link #runAsync(Statement)} execute queries in async fashion and return {@link CompletionStage} of
 * a new {@link StatementResultCursor}. Stage can be completed exceptionally when error happens, e.g. connection can't
 * be acquired from the pool.
 * <p>
 * <b>Note:</b> Returned stage can be completed by an IO thread which should never block. Otherwise IO operations on
 * this and potentially other network connections might deadlock. Please do not chain blocking operations like
 * {@link #run(String)} on the returned stage. Driver will throw {@link IllegalStateException} when blocking API
 * call is executed in IO thread. Consider using asynchronous calls throughout the chain or offloading blocking
 * operation to a different {@link Executor}. This can be done using methods with "Async" suffix like
 * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
 *
 * @see Session
 * @see Transaction
 * @since 1.0
 */
public interface StatementRunner
{
    /**
     * Run a statement and return a result stream.
     *
     * This method takes a set of parameters that will be injected into the
     * statement by Neo4j. Using parameters is highly encouraged, it helps avoid
     * dangerous cypher injection attacks and improves database performance as
     * Neo4j can re-use query plans more often.
     *
     * This particular method takes a {@link Value} as its input. This is useful
     * if you want to take a map-like value that you've gotten from a prior result
     * and send it back as parameters.
     *
     * If you are creating parameters programmatically, {@link #run(String, Map)}
     * might be more helpful, it converts your map to a {@link Value} for you.
     *
     * <h2>Example</h2>
     * <pre class="doctest:StatementRunnerDocIT#parameterTest">
     * {@code
     *
     * StatementResult cursor = session.run( "MATCH (n) WHERE n.name = {myNameParam} RETURN (n)",
     *                                       Values.parameters( "myNameParam", "Bob" ) );
     * }
     * </pre>
     *
     * @param statementTemplate text of a Neo4j statement
     * @param parameters input parameters, should be a map Value, see {@link Values#parameters(Object...)}.
     * @return a stream of result values and associated metadata
     */
    StatementResult run( String statementTemplate, Value parameters );

    /**
     * Run a statement asynchronously and return a {@link CompletionStage} with a
     * result cursor.
     * <p>
     * This method takes a set of parameters that will be injected into the
     * statement by Neo4j. Using parameters is highly encouraged, it helps avoid
     * dangerous cypher injection attacks and improves database performance as
     * Neo4j can re-use query plans more often.
     * <p>
     * This particular method takes a {@link Value} as its input. This is useful
     * if you want to take a map-like value that you've gotten from a prior result
     * and send it back as parameters.
     * <p>
     * If you are creating parameters programmatically, {@link #runAsync(String, Map)}
     * might be more helpful, it converts your map to a {@link Value} for you.
     * <h2>Example</h2>
     * <pre>
     * {@code
     *
     * CompletionStage<StatementResultCursor> cursorStage = session.runAsync(
     *             "MATCH (n) WHERE n.name = {myNameParam} RETURN (n)",
     *             Values.parameters("myNameParam", "Bob"));
     * }
     * </pre>
     * It is not allowed to chain blocking operations on the returned {@link CompletionStage}. See class javadoc for
     * more information.
     *
     * @param statementTemplate text of a Neo4j statement
     * @param parameters input parameters, should be a map Value, see {@link Values#parameters(Object...)}.
     * @return new {@link CompletionStage} that gets completed with a result cursor when query execution is successful.
     * Stage can be completed exceptionally when error happens, e.g. connection can't be acquired from the pool.
     */
    CompletionStage<StatementResultCursor> runAsync( String statementTemplate, Value parameters );

    /**
     * Run a statement and return a result stream.
     *
     * This method takes a set of parameters that will be injected into the
     * statement by Neo4j. Using parameters is highly encouraged, it helps avoid
     * dangerous cypher injection attacks and improves database performance as
     * Neo4j can re-use query plans more often.
     *
     * This version of run takes a {@link Map} of parameters. The values in the map
     * must be values that can be converted to Neo4j types. See {@link Values#parameters(Object...)} for
     * a list of allowed types.
     *
     * <h2>Example</h2>
     * <pre class="doctest:StatementRunnerDocIT#parameterTest">
     * {@code
     *
     * Map<String, Object> parameters = new HashMap<String, Object>();
     * parameters.put("myNameParam", "Bob");
     *
     * StatementResult cursor = session.run( "MATCH (n) WHERE n.name = {myNameParam} RETURN (n)",
     *                                       parameters );
     * }
     * </pre>
     *
     * @param statementTemplate text of a Neo4j statement
     * @param statementParameters input data for the statement
     * @return a stream of result values and associated metadata
     */
    StatementResult run( String statementTemplate, Map<String,Object> statementParameters );

    /**
     * Run a statement asynchronously and return a {@link CompletionStage} with a
     * result cursor.
     * <p>
     * This method takes a set of parameters that will be injected into the
     * statement by Neo4j. Using parameters is highly encouraged, it helps avoid
     * dangerous cypher injection attacks and improves database performance as
     * Neo4j can re-use query plans more often.
     * <p>
     * This version of runAsync takes a {@link Map} of parameters. The values in the map
     * must be values that can be converted to Neo4j types. See {@link Values#parameters(Object...)} for
     * a list of allowed types.
     * <h2>Example</h2>
     * <pre>
     * {@code
     *
     * Map<String, Object> parameters = new HashMap<String, Object>();
     * parameters.put("myNameParam", "Bob");
     *
     * CompletionStage<StatementResultCursor> cursorStage = session.runAsync(
     *             "MATCH (n) WHERE n.name = {myNameParam} RETURN (n)",
     *             parameters);
     * }
     * </pre>
     * It is not allowed to chain blocking operations on the returned {@link CompletionStage}. See class javadoc for
     * more information.
     *
     * @param statementTemplate text of a Neo4j statement
     * @param statementParameters input data for the statement
     * @return new {@link CompletionStage} that gets completed with a result cursor when query execution is successful.
     * Stage can be completed exceptionally when error happens, e.g. connection can't be acquired from the pool.
     */
    CompletionStage<StatementResultCursor> runAsync( String statementTemplate, Map<String,Object> statementParameters );

    /**
     * Run a statement and return a result stream.
     *
     * This method takes a set of parameters that will be injected into the
     * statement by Neo4j. Using parameters is highly encouraged, it helps avoid
     * dangerous cypher injection attacks and improves database performance as
     * Neo4j can re-use query plans more often.
     *
     * This version of run takes a {@link Record} of parameters, which can be useful
     * if you want to use the output of one statement as input for another.
     *
     * @param statementTemplate text of a Neo4j statement
     * @param statementParameters input data for the statement
     * @return a stream of result values and associated metadata
     */
    StatementResult run( String statementTemplate, Record statementParameters );

    /**
     * Run a statement asynchronously and return a {@link CompletionStage} with a
     * result cursor.
     * <p>
     * This method takes a set of parameters that will be injected into the
     * statement by Neo4j. Using parameters is highly encouraged, it helps avoid
     * dangerous cypher injection attacks and improves database performance as
     * Neo4j can re-use query plans more often.
     * <p>
     * This version of runAsync takes a {@link Record} of parameters, which can be useful
     * if you want to use the output of one statement as input for another.
     * <p>
     * It is not allowed to chain blocking operations on the returned {@link CompletionStage}. See class javadoc for
     * more information.
     *
     * @param statementTemplate text of a Neo4j statement
     * @param statementParameters input data for the statement
     * @return new {@link CompletionStage} that gets completed with a result cursor when query execution is successful.
     * Stage can be completed exceptionally when error happens, e.g. connection can't be acquired from the pool.
     */
    CompletionStage<StatementResultCursor> runAsync( String statementTemplate, Record statementParameters );

    /**
     * Run a statement and return a result stream.
     *
     * @param statementTemplate text of a Neo4j statement
     * @return a stream of result values and associated metadata
     */
    StatementResult run( String statementTemplate );

    /**
     * Run a statement asynchronously and return a {@link CompletionStage} with a
     * result cursor.
     * <p>
     * It is not allowed to chain blocking operations on the returned {@link CompletionStage}. See class javadoc for
     * more information.
     *
     * @param statementTemplate text of a Neo4j statement
     * @return new {@link CompletionStage} that gets completed with a result cursor when query execution is successful.
     * Stage can be completed exceptionally when error happens, e.g. connection can't be acquired from the pool.
     */
    CompletionStage<StatementResultCursor> runAsync( String statementTemplate );

    /**
     * Run a statement and return a result stream.
     * <h2>Example</h2>
     * <pre class="doctest:StatementRunnerDocIT#statementObjectTest">
     * {@code
     *
     * Statement statement = new Statement( "MATCH (n) WHERE n.name=$myNameParam RETURN n.age" );
     * StatementResult cursor = session.run( statement.withParameters( Values.parameters( "myNameParam", "Bob" )  ) );
     * }
     * </pre>
     *
     * @param statement a Neo4j statement
     * @return a stream of result values and associated metadata
     */
    StatementResult run( Statement statement );

    /**
     * Run a statement asynchronously and return a {@link CompletionStage} with a
     * result cursor.
     * <h2>Example</h2>
     * <pre>
     * {@code
     * Statement statement = new Statement( "MATCH (n) WHERE n.name=$myNameParam RETURN n.age" );
     * CompletionStage<StatementResultCursor> cursorStage = session.runAsync(statement);
     * }
     * </pre>
     * It is not allowed to chain blocking operations on the returned {@link CompletionStage}. See class javadoc for
     * more information.
     *
     * @param statement a Neo4j statement
     * @return new {@link CompletionStage} that gets completed with a result cursor when query execution is successful.
     * Stage can be completed exceptionally when error happens, e.g. connection can't be acquired from the pool.
     */
    CompletionStage<StatementResultCursor> runAsync( Statement statement );

    /**
     * @return type system used by this statement runner for classifying values
     */
    @Experimental
    TypeSystem typeSystem();
}
