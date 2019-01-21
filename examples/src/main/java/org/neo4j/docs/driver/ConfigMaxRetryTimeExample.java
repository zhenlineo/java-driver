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

// tag::config-max-retry-time-import[]

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

import static java.util.concurrent.TimeUnit.SECONDS;
// end::config-max-retry-time-import[]

public class ConfigMaxRetryTimeExample implements AutoCloseable
{
    private final Driver driver;

    // tag::config-max-retry-time[]
    public ConfigMaxRetryTimeExample( String uri, String user, String password )
    {
        Config config = Config.builder()
                .withMaxTransactionRetryTime( 15, SECONDS )
                .build();

        driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ), config );
    }
    // end::config-max-retry-time[]

    @Override
    public void close() throws Exception
    {
        driver.close();
    }
}
