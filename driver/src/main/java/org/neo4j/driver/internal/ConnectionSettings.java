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
package org.neo4j.driver.internal;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Session;

import static java.lang.String.format;

/**
 * The connection settings are used whenever a new connection is
 * established to a server, specifically as part of the INIT request.
 */
public class ConnectionSettings
{
    private static final String DEFAULT_USER_AGENT = format( "neo4j-java/%s", driverVersion() );

    /**
     * Extracts the driver version from the driver jar MANIFEST.MF file.
     */
    private static String driverVersion()
    {
        // "Session" is arbitrary - the only thing that matters is that the class we use here is in the
        // 'org.neo4j.driver' package, because that is where the jar manifest specifies the version.
        // This is done as part of the build, adding a MANIFEST.MF file to the generated jarfile.
        Package pkg = Session.class.getPackage();
        if ( pkg != null && pkg.getImplementationVersion() != null )
        {
            return pkg.getImplementationVersion();
        }

        // If there is no version, we're not running from a jar file, but from raw compiled class files.
        // This should only happen during development, so call the version 'dev'.
        return "dev";
    }

    private final AuthToken authToken;
    private final String userAgent;
    private final int connectTimeoutMillis;

    public ConnectionSettings( AuthToken authToken, String userAgent, int connectTimeoutMillis )
    {
        this.authToken = authToken;
        this.userAgent = userAgent;
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    public ConnectionSettings( AuthToken authToken, int connectTimeoutMillis )
    {
        this( authToken, DEFAULT_USER_AGENT, connectTimeoutMillis );
    }

    public AuthToken authToken()
    {
        return authToken;
    }

    public String userAgent()
    {
        return userAgent;
    }

    public int connectTimeoutMillis()
    {
        return connectTimeoutMillis;
    }
}
