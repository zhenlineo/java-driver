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
package org.neo4j.driver.internal.messaging.request;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;

import static org.neo4j.driver.internal.util.MetadataExtractor.ABSENT_STATEMENT_ID;

/**
 * PULL_N request message
 * <p>
 * Sent by clients to pull the entirety of the remaining stream down.
 */
public class PullNMessage extends PullAllMessage
{
    private final Map<String,Value> metadata = new HashMap<>();

    public PullNMessage( long n, long id )
    {
        super();
        this.metadata.put( "n", Values.value( n ) );
        if ( id != ABSENT_STATEMENT_ID )
        {
            this.metadata.put( "stmt_id", Values.value( id ) );
        }
    }


    public Map<String,Value> metadata()
    {
        return metadata;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        PullNMessage that = (PullNMessage) o;
        return Objects.equals( metadata, that.metadata );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( metadata );
    }

    @Override
    public String toString()
    {
        return "PULL_N " + metadata;
    }
}
