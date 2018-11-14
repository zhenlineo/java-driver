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
package org.neo4j.driver.v1;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import org.neo4j.driver.v1.exceptions.NoSuchRecordException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.util.Consumer;
import org.neo4j.driver.v1.util.Function;

/**
 * The result of asynchronous execution of a Cypher statement, conceptually an asynchronous stream of
 * {@link Record records}.
 * <p>
 * Result can be eagerly fetched in a list using {@link #listAsync()} or navigated lazily using
 * {@link #forEachAsync(Consumer)} or {@link #nextAsync()}.
 * <p>
 * Results are valid until the next statement is run or until the end of the current transaction,
 * whichever comes first. To keep a result around while further statements are run, or to use a result outside the scope
 * of the current transaction, see {@link #listAsync()}.
 * <h2>Important note on semantics</h2>
 * <p>
 * In order to handle very large results, and to minimize memory overhead and maximize
 * performance, results are retrieved lazily. Please see {@link StatementRunner} for
 * important details on the effects of this.
 * <p>
 * The short version is that, if you want a hard guarantee that the underlying statement
 * has completed, you need to either call {@link Transaction#commitAsync()} on the {@link Transaction transaction}
 * or {@link Session#closeAsync()} on the {@link Session session} that created this result, or you need to use
 * the result.
 * <p>
 * <b>Note:</b> Every returned {@link CompletionStage} can be completed by an IO thread which should never block.
 * Otherwise IO operations on this and potentially other network connections might deadlock. Please do not chain
 * blocking operations like {@link Session#run(String)} on the returned stages. Driver will throw
 * {@link IllegalStateException} when blocking API call is executed in IO thread. Consider using asynchronous calls
 * throughout the chain or offloading blocking operation to a different {@link Executor}. This can be done using
 * methods with "Async" suffix like {@link CompletionStage#thenApplyAsync(java.util.function.Function)} or
 * {@link CompletionStage#thenApplyAsync(java.util.function.Function, Executor)}.
 *
 * @since 1.5
 */
public interface StatementResultCursor
{
    /**
     * Retrieve the keys of the records this result cursor contains.
     *
     * @return list of all keys.
     */
    List<String> keys();

    /**
     * Asynchronously retrieve the result summary.
     * <p>
     * If the records in the result is not fully consumed, then calling this method will force to pull all remaining
     * records into buffer to yield the summary.
     * <p>
     * If you want to obtain the summary but discard the records, use {@link #consumeAsync()} instead.
     *
     * @return a {@link CompletionStage} completed with a summary for the whole query result. Stage can also be
     * completed exceptionally if query execution fails.
     */
    CompletionStage<ResultSummary> summaryAsync();

    /**
     * Asynchronously navigate to and retrieve the next {@link Record} in this result. Returned stage can contain
     * {@code null} if end of records stream has been reached.
     *
     * @return a {@link CompletionStage} completed with a record or {@code null}. Stage can also be
     * completed exceptionally if query execution fails.
     */
    CompletionStage<Record> nextAsync();

    /**
     * Asynchronously investigate the next upcoming {@link Record} without moving forward in the result. Returned
     * stage can contain {@code null} if end of records stream has been reached.
     *
     * @return a {@link CompletionStage} completed with a record or {@code null}. Stage can also be
     * completed exceptionally if query execution fails.
     */
    CompletionStage<Record> peekAsync();

    /**
     * Asynchronously return the first record in the result, failing if there is not exactly
     * one record left in the stream.
     *
     * @return a {@link CompletionStage} completed with the first and only record in the stream. Stage will be
     * completed exceptionally with {@link NoSuchRecordException} if there is not exactly one record left in the
     * stream. It can also be completed exceptionally if query execution fails.
     */
    CompletionStage<Record> singleAsync();

    /**
     * Asynchronously consume the entire result, yielding a summary of it. Calling this method exhausts the result.
     *
     * @return a {@link CompletionStage} completed with a summary for the whole query result. Stage can also be
     * completed exceptionally if query execution fails.
     */
    CompletionStage<ResultSummary> consumeAsync();

    /**
     * Asynchronously apply the given {@link Consumer action} to every record in the result, yielding a summary of it.
     *
     * @param action the function to be applied to every record in the result. Provided function should not block.
     * @return a {@link CompletionStage} completed with a summary for the whole query result. Stage can also be
     * completed exceptionally if query execution or provided function fails.
     */
    CompletionStage<ResultSummary> forEachAsync( Consumer<Record> action );

    /**
     * Asynchronously retrieve and store the entire result stream.
     * This can be used if you want to iterate over the stream multiple times or to store the
     * whole result for later use.
     * <p>
     * Note that this method can only be used if you know that the statement that
     * yielded this result returns a finite stream. Some statements can yield
     * infinite results, in which case calling this method will lead to running
     * out of memory.
     * <p>
     * Calling this method exhausts the result.
     *
     * @return a {@link CompletionStage} completed with a list of all remaining immutable records. Stage can also be
     * completed exceptionally if query execution fails.
     */
    CompletionStage<List<Record>> listAsync();

    /**
     * Asynchronously retrieve and store a projection of the entire result.
     * This can be used if you want to iterate over the stream multiple times or to store the
     * whole result for later use.
     * <p>
     * Note that this method can only be used if you know that the statement that
     * yielded this result returns a finite stream. Some statements can yield
     * infinite results, in which case calling this method will lead to running
     * out of memory.
     * <p>
     * Calling this method exhausts the result.
     *
     * @param mapFunction a function to map from Record to T. See {@link Records} for some predefined functions.
     * @param <T> the type of result list elements
     * @return a {@link CompletionStage} completed with a list of all remaining immutable records. Stage can also be
     * completed exceptionally if query execution or provided function fails.
     */
    <T> CompletionStage<List<T>> listAsync( Function<Record,T> mapFunction );
}
