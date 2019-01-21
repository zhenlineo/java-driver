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
package org.neo4j.driver.internal.messaging;

import java.io.IOException;
import java.util.Map;

import org.neo4j.driver.internal.messaging.encode.DiscardAllMessageEncoder;
import org.neo4j.driver.internal.messaging.encode.FailureMessageEncoder;
import org.neo4j.driver.internal.messaging.encode.IgnoredMessageEncoder;
import org.neo4j.driver.internal.messaging.encode.InitMessageEncoder;
import org.neo4j.driver.internal.messaging.encode.PullAllMessageEncoder;
import org.neo4j.driver.internal.messaging.encode.RecordMessageEncoder;
import org.neo4j.driver.internal.messaging.encode.ResetMessageEncoder;
import org.neo4j.driver.internal.messaging.encode.RunMessageEncoder;
import org.neo4j.driver.internal.messaging.encode.SuccessMessageEncoder;
import org.neo4j.driver.internal.messaging.request.DiscardAllMessage;
import org.neo4j.driver.internal.messaging.request.InitMessage;
import org.neo4j.driver.internal.messaging.request.PullAllMessage;
import org.neo4j.driver.internal.messaging.request.ResetMessage;
import org.neo4j.driver.internal.messaging.request.RunMessage;
import org.neo4j.driver.internal.messaging.response.FailureMessage;
import org.neo4j.driver.internal.messaging.response.IgnoredMessage;
import org.neo4j.driver.internal.messaging.response.RecordMessage;
import org.neo4j.driver.internal.messaging.response.SuccessMessage;
import org.neo4j.driver.internal.messaging.v2.MessageFormatV2;
import org.neo4j.driver.internal.messaging.v2.ValuePackerV2;
import org.neo4j.driver.internal.packstream.PackOutput;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.internal.value.InternalValue;
import org.neo4j.driver.types.Entity;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;

/**
 * This class provides the missing server side packing methods to serialize Node, Relationship and Path.
 * It also allows writing of server side messages like SUCCESS, FAILURE, IGNORED and RECORD.
 */
public class KnowledgeableMessageFormat extends MessageFormatV2
{
    @Override
    public MessageFormat.Writer newWriter( PackOutput output, boolean byteArraySupportEnabled )
    {
        return new KnowledgeableMessageWriter( output );
    }

    private static class KnowledgeableMessageWriter extends AbstractMessageWriter
    {
        KnowledgeableMessageWriter( PackOutput output )
        {
            super( new KnowledgeableValuePacker( output ), buildEncoders() );
        }

        static Map<Byte,MessageEncoder> buildEncoders()
        {
            Map<Byte,MessageEncoder> result = Iterables.newHashMapWithSize( 10 );
            // request message encoders
            result.put( DiscardAllMessage.SIGNATURE, new DiscardAllMessageEncoder() );
            result.put( InitMessage.SIGNATURE, new InitMessageEncoder() );
            result.put( PullAllMessage.SIGNATURE, new PullAllMessageEncoder() );
            result.put( ResetMessage.SIGNATURE, new ResetMessageEncoder() );
            result.put( RunMessage.SIGNATURE, new RunMessageEncoder() );
            // response message encoders
            result.put( FailureMessage.SIGNATURE, new FailureMessageEncoder() );
            result.put( IgnoredMessage.SIGNATURE, new IgnoredMessageEncoder() );
            result.put( RecordMessage.SIGNATURE, new RecordMessageEncoder() );
            result.put( SuccessMessage.SIGNATURE, new SuccessMessageEncoder() );
            return result;
        }
    }

    private static class KnowledgeableValuePacker extends ValuePackerV2
    {
        KnowledgeableValuePacker( PackOutput output )
        {
            super( output );
        }

        @Override
        protected void packInternalValue( InternalValue value ) throws IOException
        {
            TypeConstructor typeConstructor = value.typeConstructor();
            switch ( typeConstructor )
            {
            case NODE:
                Node node = value.asNode();
                packNode( node );
                break;

            case RELATIONSHIP:
                Relationship rel = value.asRelationship();
                packRelationship( rel );
                break;

            case PATH:
                Path path = value.asPath();
                packPath( path );
                break;
            default:
                super.packInternalValue( value );
            }
        }

        private void packPath( Path path ) throws IOException
        {
            packer.packStructHeader( 3, PATH );

            // Unique nodes
            Map<Node,Integer> nodeIdx = Iterables.newLinkedHashMapWithSize( path.length() + 1 );
            for ( Node node : path.nodes() )
            {
                if ( !nodeIdx.containsKey( node ) )
                {
                    nodeIdx.put( node, nodeIdx.size() );
                }
            }
            packer.packListHeader( nodeIdx.size() );
            for ( Node node : nodeIdx.keySet() )
            {
                packNode( node );
            }

            // Unique rels
            Map<Relationship,Integer> relIdx = Iterables.newLinkedHashMapWithSize( path.length() );
            for ( Relationship rel : path.relationships() )
            {
                if ( !relIdx.containsKey( rel ) )
                {
                    relIdx.put( rel, relIdx.size() + 1 );
                }
            }
            packer.packListHeader( relIdx.size() );
            for ( Relationship rel : relIdx.keySet() )
            {
                packer.packStructHeader( 3, UNBOUND_RELATIONSHIP );
                packer.pack( rel.id() );
                packer.pack( rel.type() );
                packProperties( rel );
            }

            // Sequence
            packer.packListHeader( path.length() * 2 );
            for ( Path.Segment seg : path )
            {
                Relationship rel = seg.relationship();
                long relEndId = rel.endNodeId();
                long segEndId = seg.end().id();
                int size = relEndId == segEndId ? relIdx.get( rel ) : -relIdx.get( rel );
                packer.pack( size );
                packer.pack( nodeIdx.get( seg.end() ) );
            }
        }

        private void packRelationship( Relationship rel ) throws IOException
        {
            packer.packStructHeader( 5, RELATIONSHIP );
            packer.pack( rel.id() );
            packer.pack( rel.startNodeId() );
            packer.pack( rel.endNodeId() );

            packer.pack( rel.type() );

            packProperties( rel );
        }

        private void packNode( Node node ) throws IOException
        {
            packer.packStructHeader( NODE_FIELDS, NODE );
            packer.pack( node.id() );

            Iterable<String> labels = node.labels();
            packer.packListHeader( Iterables.count( labels ) );
            for ( String label : labels )
            {
                packer.pack( label );
            }

            packProperties( node );
        }

        private void packProperties( Entity entity ) throws IOException
        {
            Iterable<String> keys = entity.keys();
            packer.packMapHeader( entity.size() );
            for ( String propKey : keys )
            {
                packer.pack( propKey );
                packInternalValue( (InternalValue) entity.get( propKey ) );
            }
        }
    }
}
