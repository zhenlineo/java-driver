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
package org.neo4j.driver.internal.handlers;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.internal.async.inbound.ChannelErrorHandler;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.async.outbound.OutboundMessageHandler;
import org.neo4j.driver.internal.messaging.v1.MessageFormatV1;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.UntrustedServerException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.async.ChannelAttributes.connectionId;
import static org.neo4j.driver.internal.async.ChannelAttributes.serverVersion;
import static org.neo4j.driver.internal.async.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.async.outbound.OutboundMessageHandler.NAME;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.Values.value;

class HelloResponseHandlerTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @BeforeEach
    void setUp()
    {
        setMessageDispatcher( channel, new InboundMessageDispatcher( channel, DEV_NULL_LOGGING ) );
        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast( NAME, new OutboundMessageHandler( new MessageFormatV1(), DEV_NULL_LOGGING ) );
        pipeline.addLast( new ChannelErrorHandler( DEV_NULL_LOGGING ) );
    }

    @AfterEach
    void tearDown()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldSetServerVersionOnChannel()
    {
        ChannelPromise channelPromise = channel.newPromise();
        HelloResponseHandler handler = new HelloResponseHandler( channelPromise );

        Map<String,Value> metadata = metadata( ServerVersion.v3_2_0, "bolt-1" );
        handler.onSuccess( metadata );

        assertTrue( channelPromise.isSuccess() );
        assertEquals( ServerVersion.v3_2_0, serverVersion( channel ) );
    }

    @Test
    void shouldThrowWhenServerVersionNotReturned()
    {
        ChannelPromise channelPromise = channel.newPromise();
        HelloResponseHandler handler = new HelloResponseHandler( channelPromise );

        Map<String,Value> metadata = metadata( null, "bolt-1" );
        assertThrows( UntrustedServerException.class, () -> handler.onSuccess( metadata ) );

        assertFalse( channelPromise.isSuccess() ); // initialization failed
        assertTrue( channel.closeFuture().isDone() ); // channel was closed
    }

    @Test
    void shouldThrowWhenServerVersionIsNull()
    {
        ChannelPromise channelPromise = channel.newPromise();
        HelloResponseHandler handler = new HelloResponseHandler( channelPromise );

        Map<String,Value> metadata = metadata( Values.NULL, "bolt-x" );
        assertThrows( UntrustedServerException.class, () -> handler.onSuccess( metadata ) );

        assertFalse( channelPromise.isSuccess() ); // initialization failed
        assertTrue( channel.closeFuture().isDone() ); // channel was closed
    }

    @Test
    void shouldThrowWhenServerVersionCantBeParsed()
    {
        ChannelPromise channelPromise = channel.newPromise();
        HelloResponseHandler handler = new HelloResponseHandler( channelPromise );

        Map<String,Value> metadata = metadata( "WrongServerVersion", "bolt-x" );
        assertThrows( IllegalArgumentException.class, () -> handler.onSuccess( metadata ) );

        assertFalse( channelPromise.isSuccess() ); // initialization failed
        assertTrue( channel.closeFuture().isDone() ); // channel was closed
    }

    @Test
    void shouldSetConnectionIdOnChannel()
    {
        ChannelPromise channelPromise = channel.newPromise();
        HelloResponseHandler handler = new HelloResponseHandler( channelPromise );

        Map<String,Value> metadata = metadata( ServerVersion.v3_2_0, "bolt-42" );
        handler.onSuccess( metadata );

        assertTrue( channelPromise.isSuccess() );
        assertEquals( "bolt-42", connectionId( channel ) );
    }

    @Test
    void shouldThrowWhenConnectionIdNotReturned()
    {
        ChannelPromise channelPromise = channel.newPromise();
        HelloResponseHandler handler = new HelloResponseHandler( channelPromise );

        Map<String,Value> metadata = metadata( ServerVersion.v3_1_0, null );
        assertThrows( IllegalStateException.class, () -> handler.onSuccess( metadata ) );

        assertFalse( channelPromise.isSuccess() ); // initialization failed
        assertTrue( channel.closeFuture().isDone() ); // channel was closed
    }

    @Test
    void shouldThrowWhenConnectionIdIsNull()
    {
        ChannelPromise channelPromise = channel.newPromise();
        HelloResponseHandler handler = new HelloResponseHandler( channelPromise );

        Map<String,Value> metadata = metadata( ServerVersion.v3_2_0, Values.NULL );
        assertThrows( IllegalStateException.class, () -> handler.onSuccess( metadata ) );

        assertFalse( channelPromise.isSuccess() ); // initialization failed
        assertTrue( channel.closeFuture().isDone() ); // channel was closed
    }

    @Test
    void shouldCloseChannelOnFailure() throws Exception
    {
        ChannelPromise channelPromise = channel.newPromise();
        HelloResponseHandler handler = new HelloResponseHandler( channelPromise );

        RuntimeException error = new RuntimeException( "Hi!" );
        handler.onFailure( error );

        ChannelFuture channelCloseFuture = channel.closeFuture();
        channelCloseFuture.await( 5, TimeUnit.SECONDS );

        assertTrue( channelCloseFuture.isSuccess() );
        assertTrue( channelPromise.isDone() );
        assertEquals( error, channelPromise.cause() );
    }

    private static Map<String,Value> metadata( Object version, Object connectionId )
    {
        Map<String,Value> result = new HashMap<>();

        if ( version == null )
        {
            result.put( "server", null );
        }
        else if ( version instanceof Value && ((Value) version).isNull() )
        {
            result.put( "server", Values.NULL );
        }
        else
        {
            result.put( "server", value( version.toString() ) );
        }

        if ( connectionId == null )
        {
            result.put( "connection_id", null );
        }
        else
        {
            result.put( "connection_id", value( connectionId ) );
        }

        return result;
    }
}
