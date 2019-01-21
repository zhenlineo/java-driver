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
package org.neo4j.driver.internal.messaging.encode;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.internal.Bookmarks;
import org.neo4j.driver.internal.messaging.ValuePacker;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.Value;

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.internal.messaging.request.DiscardAllMessage.DISCARD_ALL;
import static org.neo4j.driver.Values.value;

class RunWithMetadataMessageEncoderTest
{
    private final RunWithMetadataMessageEncoder encoder = new RunWithMetadataMessageEncoder();
    private final ValuePacker packer = mock( ValuePacker.class );

    @Test
    void shouldEncodeRunWithMetadataMessage() throws Exception
    {
        Map<String,Value> params = singletonMap( "answer", value( 42 ) );

        Bookmarks bookmarks = Bookmarks.from( "neo4j:bookmark:v1:tx999" );

        Map<String,Value> txMetadata = new HashMap<>();
        txMetadata.put( "key1", value( "value1" ) );
        txMetadata.put( "key2", value( 1, 2, 3, 4, 5 ) );
        txMetadata.put( "key3", value( true ) );

        Duration txTimeout = Duration.ofMillis( 42 );

        encoder.encode( new RunWithMetadataMessage( "RETURN $answer", params, bookmarks, txTimeout, txMetadata ), packer );

        InOrder order = inOrder( packer );
        order.verify( packer ).packStructHeader( 3, RunWithMetadataMessage.SIGNATURE );
        order.verify( packer ).pack( "RETURN $answer" );
        order.verify( packer ).pack( params );

        Map<String,Value> expectedMetadata = new HashMap<>();
        expectedMetadata.put( "bookmarks", value( bookmarks.values() ) );
        expectedMetadata.put( "tx_timeout", value( 42 ) );
        expectedMetadata.put( "tx_metadata", value( txMetadata ) );

        order.verify( packer ).pack( expectedMetadata );
    }

    @Test
    void shouldFailToEncodeWrongMessage()
    {
        assertThrows( IllegalArgumentException.class, () -> encoder.encode( DISCARD_ALL, packer ) );
    }
}
