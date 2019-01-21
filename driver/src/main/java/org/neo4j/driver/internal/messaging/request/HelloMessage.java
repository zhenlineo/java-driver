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
package org.neo4j.driver.internal.messaging.request;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.Value;

import static org.neo4j.driver.internal.security.InternalAuthToken.CREDENTIALS_KEY;
import static org.neo4j.driver.Values.value;

public class HelloMessage implements Message
{
    public final static byte SIGNATURE = 0x01;

    private static final String USER_AGENT_METADATA_KEY = "user_agent";

    private final Map<String,Value> metadata;

    public HelloMessage( String userAgent, Map<String,Value> authToken )
    {
        this.metadata = buildMetadata( userAgent, authToken );
    }

    public Map<String,Value> metadata()
    {
        return metadata;
    }

    @Override
    public byte signature()
    {
        return SIGNATURE;
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
        HelloMessage that = (HelloMessage) o;
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
        Map<String,Value> metadataCopy = new HashMap<>( metadata );
        metadataCopy.replace( CREDENTIALS_KEY, value( "******" ) );
        return "HELLO " + metadataCopy;
    }

    private static Map<String,Value> buildMetadata( String userAgent, Map<String,Value> authToken )
    {
        Map<String,Value> result = new HashMap<>( authToken );
        result.put( USER_AGENT_METADATA_KEY, value( userAgent ) );
        return result;
    }
}
