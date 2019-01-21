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
package org.neo4j.driver.internal.async;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import org.neo4j.driver.internal.messaging.v1.BoltProtocolV1;
import org.neo4j.driver.internal.messaging.v2.BoltProtocolV2;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.driver.internal.async.BoltProtocolUtil.BOLT_MAGIC_PREAMBLE;
import static org.neo4j.driver.internal.async.BoltProtocolUtil.NO_PROTOCOL_VERSION;
import static org.neo4j.driver.internal.async.BoltProtocolUtil.handshakeBuf;
import static org.neo4j.driver.internal.async.BoltProtocolUtil.handshakeString;
import static org.neo4j.driver.internal.async.BoltProtocolUtil.writeChunkHeader;
import static org.neo4j.driver.internal.async.BoltProtocolUtil.writeEmptyChunkHeader;
import static org.neo4j.driver.internal.async.BoltProtocolUtil.writeMessageBoundary;
import static org.neo4j.driver.util.TestUtil.assertByteBufContains;

class BoltProtocolUtilTest
{
    @Test
    void shouldReturnHandshakeBuf()
    {
        assertByteBufContains(
                handshakeBuf(),
                BOLT_MAGIC_PREAMBLE, BoltProtocolV3.VERSION, BoltProtocolV2.VERSION, BoltProtocolV1.VERSION, NO_PROTOCOL_VERSION
        );
    }

    @Test
    void shouldReturnHandshakeString()
    {
        assertEquals( "[0x6060b017, 3, 2, 1, 0]", handshakeString() );
    }

    @Test
    void shouldWriteMessageBoundary()
    {
        ByteBuf buf = Unpooled.buffer();

        buf.writeInt( 1 );
        buf.writeInt( 2 );
        buf.writeInt( 3 );
        writeMessageBoundary( buf );

        assertByteBufContains( buf, 1, 2, 3, (byte) 0, (byte) 0 );
    }

    @Test
    void shouldWriteEmptyChunkHeader()
    {
        ByteBuf buf = Unpooled.buffer();

        writeEmptyChunkHeader( buf );
        buf.writeInt( 1 );
        buf.writeInt( 2 );
        buf.writeInt( 3 );

        assertByteBufContains( buf, (byte) 0, (byte) 0, 1, 2, 3 );
    }

    @Test
    void shouldWriteChunkHeader()
    {
        ByteBuf buf = Unpooled.buffer();

        writeEmptyChunkHeader( buf );
        buf.writeInt( 1 );
        buf.writeInt( 2 );
        buf.writeInt( 3 );
        writeChunkHeader( buf, 0, 42 );

        assertByteBufContains( buf, (short) 42, 1, 2, 3 );
    }
}
