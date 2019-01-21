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
package org.neo4j.docs.driver;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Session;

import static java.util.Collections.singletonMap;

public class Slf4jLoggingExample implements AutoCloseable
{
    private final Driver driver;

    public Slf4jLoggingExample( String uri, String user, String password )
    {
        Config config = Config.builder()
                .withLogging( Logging.slf4j() )
                .build();

        driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ), config );
    }

    public Object runReturnQuery( Object value )
    {
        try ( Session session = driver.session() )
        {
            return session.run( "RETURN $x", singletonMap( "x", value ) ).single().get( 0 ).asObject();
        }
    }

    @Override
    public void close()
    {
        driver.close();
    }
}
