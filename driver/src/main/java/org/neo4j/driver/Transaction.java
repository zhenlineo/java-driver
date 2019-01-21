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

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.neo4j.driver.util.Resource;

/**
 * Logical container for an atomic unit of work.
 * A driver Transaction object corresponds to a server transaction.
 * <p>
 * Blocking API:
 * <p>
 * Transactions are typically wrapped in a try-with-resources block
 * which ensures that <code>COMMIT</code> or <code>ROLLBACK</code>
 * occurs correctly on close. Note that <code>ROLLBACK</code> is the
 * default action unless {@link #success()} is called before closing.
 * <pre class="docTest:TransactionDocIT#classDoc">
 * {@code
 * try(Transaction tx = session.beginTransaction())
 * {
 *     tx.run("CREATE (a:Person {name: {x}})", parameters("x", "Alice"));
 *     tx.success();
 * }
 * }
 * </pre>
 * Blocking calls are: {@link #success()}, {@link #failure()}, {@link #close()}
 * and various overloads of {@link #run(Statement)}.
 * <p>
 * Asynchronous API:
 * <p>
 * Transactions are typically obtained in a {@link CompletionStage} and all
 * operations chain on this stage. Explicit commit with {@link #commitAsync()}
 * or rollback with {@link #rollbackAsync()} is required. Without explicit
 * commit/rollback corresponding transaction will remain open in the database.
 * <pre>
 * {@code
 * session.beginTransactionAsync()
 *        .thenCompose(tx ->
 *               tx.runAsync("CREATE (a:Person {name: {x}})", parameters("x", "Alice"))
 *                 .exceptionally(e -> {
 *                    e.printStackTrace();
 *                    return null;
 *                 })
 *                 .thenApply(ignore -> tx)
 *        ).thenCompose(Transaction::commitAsync);
 * }
 * </pre>
 * Async calls are: {@link #commitAsync()}, {@link #rollbackAsync()} and various overloads of
 * {@link #runAsync(Statement)}.
 *
 * @see Session#run
 * @see StatementRunner
 * @since 1.0
 */
public interface Transaction extends Resource, StatementRunner
{
    /**
     * Mark this transaction as successful. You must call this method before calling {@link #close()} to have your
     * transaction committed.
     */
    void success();

    /**
     * Mark this transaction as failed. When you call {@link #close()}, the transaction will value rolled back.
     *
     * After this method has been called, there is nothing that can be done to "un-mark" it. This is a safety feature
     * to make sure no other code calls {@link #success()} and makes a transaction commit that was meant to be rolled
     * back.
     *
     * Example:
     *
     * <pre class="docTest:TransactionDocIT#failure">
     * {@code
     * try(Transaction tx = session.beginTransaction() )
     * {
     *     tx.run("CREATE (a:Person {name: {x}})", parameters("x", "Alice"));
     *     tx.failure();
     * }
     * }
     * </pre>
     */
    void failure();

    /**
     * Closing the transaction will complete it - it will commit if {@link #success()} has been called.
     * When this method returns, all outstanding statements in the transaction are guaranteed to
     * have completed, meaning any writes you performed are guaranteed to be durably stored.
     */
    @Override
    void close();

    /**
     * Commit this transaction in asynchronous fashion. This operation is typically executed as part of the
     * {@link CompletionStage} chain that starts with a transaction. It is logically equivalent to a combination of
     * blocking {@link #success()} and {@link #close()}. However, it is asynchronous and returns new
     * {@link CompletionStage}. There is no need to close transaction after calling this method. Transaction object
     * should not be used after calling this method.
     * <p>
     * Returned stage can be completed by an IO thread which should never block. Otherwise IO operations on this and
     * potentially other network connections might deadlock. Please do not chain blocking operations like
     * {@link #run(String)} on the returned stage. Driver will throw {@link IllegalStateException} when blocking API
     * call is executed in IO thread. Consider using asynchronous calls throughout the chain or offloading blocking
     * operation to a different {@link Executor}. This can be done using methods with "Async" suffix like
     * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
     *
     * @return new {@link CompletionStage} that gets completed with {@code null} when commit is successful. Stage can
     * be completed exceptionally when commit fails.
     */
    CompletionStage<Void> commitAsync();

    /**
     * Rollback this transaction in asynchronous fashion. This operation is typically executed as part of the
     * {@link CompletionStage} chain that starts with a transaction. It is logically equivalent to a combination of
     * blocking {@link #failure()} and {@link #close()}. However, it is asynchronous and returns new
     * {@link CompletionStage}. There is no need to close transaction after calling this method. Transaction object
     * should not be used after calling this method.
     * <p>
     * Returned stage can be completed by an IO thread which should never block. Otherwise IO operations on this and
     * potentially other network connections might deadlock. Please do not chain blocking operations like
     * {@link #run(String)} on the returned stage. Driver will throw {@link IllegalStateException} when blocking API
     * call is executed in IO thread. Consider using asynchronous calls throughout the chain or offloading blocking
     * operation to a different {@link Executor}. This can be done using methods with "Async" suffix like
     * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
     *
     * @return new {@link CompletionStage} that gets completed with {@code null} when rollback is successful. Stage can
     * be completed exceptionally when rollback fails.
     */
    CompletionStage<Void> rollbackAsync();
}
