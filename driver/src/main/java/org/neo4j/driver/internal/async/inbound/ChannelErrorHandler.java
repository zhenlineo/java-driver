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
package org.neo4j.driver.internal.async.inbound;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.CodecException;

import java.io.IOException;

import org.neo4j.driver.internal.logging.ChannelActivityLogger;
import org.neo4j.driver.internal.util.ErrorUtil;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.ServiceUnavailableException;

import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.async.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.async.ChannelAttributes.terminationReason;

public class ChannelErrorHandler extends ChannelInboundHandlerAdapter
{
    private final Logging logging;

    private InboundMessageDispatcher messageDispatcher;
    private Logger log;
    private boolean failed;

    public ChannelErrorHandler( Logging logging )
    {
        this.logging = logging;
    }

    @Override
    public void handlerAdded( ChannelHandlerContext ctx )
    {
        messageDispatcher = requireNonNull( messageDispatcher( ctx.channel() ) );
        log = new ChannelActivityLogger( ctx.channel(), logging, getClass() );
    }

    @Override
    public void handlerRemoved( ChannelHandlerContext ctx )
    {
        messageDispatcher = null;
        log = null;
        failed = false;
    }

    @Override
    public void channelInactive( ChannelHandlerContext ctx )
    {
        log.debug( "Channel is inactive" );

        if ( !failed )
        {
            // channel became inactive not because of a fatal exception that came from exceptionCaught
            // it is most likely inactive because actual network connection broke or was explicitly closed by the driver

            String terminationReason = terminationReason( ctx.channel() );
            ServiceUnavailableException error = ErrorUtil.newConnectionTerminatedError( terminationReason );
            fail( ctx, error );
        }
    }

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable error )
    {
        if ( failed )
        {
            log.warn( "Another fatal error occurred in the pipeline", error );
        }
        else
        {
            failed = true;
            log.error( "Fatal error occurred in the pipeline", error );
            fail( ctx, error );
        }
    }

    private void fail( ChannelHandlerContext ctx, Throwable error )
    {
        Throwable cause = transformError( error );
        messageDispatcher.handleFatalError( cause );
        log.debug( "Closing channel because of a failure '%s'", error );
        ctx.close();
    }

    private static Throwable transformError( Throwable error )
    {
        if ( error instanceof CodecException && error.getCause() != null )
        {
            // unwrap the CodecException if it has a cause
            error = error.getCause();
        }

        if ( error instanceof IOException )
        {
            return new ServiceUnavailableException( "Connection to the database failed", error );
        }
        else
        {
            return error;
        }
    }
}
