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
package org.neo4j.driver.stress;

import java.net.URI;

import org.neo4j.driver.Config;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.SecurityException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.AuthTokens.basic;

public class FailedAuth<C extends AbstractContext> implements BlockingCommand<C>
{
    private final URI clusterUri;
    private final Logging logging;

    public FailedAuth( URI clusterUri, Logging logging )
    {
        this.clusterUri = clusterUri;
        this.logging = logging;
    }

    @Override
    public void execute( C context )
    {
        Config config = Config.builder().withLogging( logging ).build();

        SecurityException e = assertThrows( SecurityException.class,
                () -> GraphDatabase.driver( clusterUri, basic( "wrongUsername", "wrongPassword" ), config ) );
        assertThat( e.getMessage(), containsString( "authentication failure" ) );
    }
}
