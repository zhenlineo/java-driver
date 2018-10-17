package org.neo4j.driver.internal.handlers;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.util.Function;

public interface PullHandler extends ResponseHandler
{
    CompletionStage<ResultSummary> summaryAsync();

    CompletionStage<Record> nextAsync();

    CompletionStage<Record> peekAsync();

    CompletionStage<ResultSummary> consumeAsync();

    <T> CompletionStage<List<T>> listAsync( Function<Record, T> mapFunction );

    CompletionStage<Throwable> failureAsync();

    void pull(long size);

    void cancel();
}
