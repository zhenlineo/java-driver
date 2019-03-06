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
package org.neo4j.driver.internal.handlers.pulln;

import java.util.Map;

import org.neo4j.driver.internal.BookmarksHolder;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.MetadataExtractor;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;

import static java.util.Objects.requireNonNull;

public class SessionPullResponseHandler extends AbstractBasicPullResponseHandler
{
    private final BookmarksHolder bookmarksHolder;

    public SessionPullResponseHandler( Statement statement, RunResponseHandler runResponseHandler,
            Connection connection, BookmarksHolder bookmarksHolder, MetadataExtractor metadataExtractor )
    {
        super( statement, runResponseHandler, connection, metadataExtractor );
        this.bookmarksHolder = requireNonNull( bookmarksHolder );
    }

    @Override
    protected void afterSuccess( Map<String,Value> metadata )
    {
        releaseConnection();
        bookmarksHolder.setBookmarks( metadataExtractor.extractBookmarks( metadata ) );
    }

    @Override
    protected void afterFailure( Throwable error )
    {
        releaseConnection();
    }

    private void releaseConnection()
    {
        connection.release(); // release in background
    }
}
