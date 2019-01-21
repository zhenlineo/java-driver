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
package org.neo4j.driver.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

public class DaemonThreadFactory implements ThreadFactory
{
    private final String namePrefix;
    private final AtomicInteger threadId;

    public DaemonThreadFactory( String namePrefix )
    {
        this.namePrefix = requireNonNull( namePrefix );
        this.threadId = new AtomicInteger();
    }

    public static ThreadFactory daemon( String namePrefix )
    {
        return new DaemonThreadFactory( namePrefix );
    }

    @Override
    public Thread newThread( Runnable runnable )
    {
        Thread thread = new Thread( runnable );
        thread.setName( namePrefix + threadId.incrementAndGet() );
        thread.setDaemon( true );
        return thread;
    }
}
