/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.driver.v1.internal;

import org.junit.Test;

import java.util.List;

import org.neo4j.driver.v1.Node;
import org.neo4j.driver.v1.internal.util.Iterables;
import org.neo4j.driver.v1.Values;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.neo4j.driver.v1.Values.parameters;

public class SelfContainedNodeTest
{

    private Node adamTheNode()
    {
        return new SimpleNode( 1, asList( "Person" ),
                parameters( "name", Values.value( "Adam" ) ) );
    }

    @Test
    public void testIdentity()
    {
        // Given
        Node node = adamTheNode();

        // Then
        assertThat( node.identity(), equalTo( Identities.identity( 1 ) ) );
    }

    @Test
    public void testLabels()
    {
        // Given
        Node node = adamTheNode();

        // Then
        List<String> labels = Iterables.toList( node.labels() );
        assertThat( labels.size(), equalTo( 1 ) );
        assertThat( labels.contains( "Person" ), equalTo( true ) );
    }

    @Test
    public void testKeys()
    {
        // Given
        Node node = adamTheNode();

        // Then
        List<String> keys = Iterables.toList( node.propertyKeys() );
        assertThat( keys.size(), equalTo( 1 ) );
        assertThat( keys.contains( "name" ), equalTo( true ) );
    }

    @Test
    public void testValue()
    {
        // Given
        Node node = adamTheNode();

        // Then
        assertThat( node.property( "name" ).javaString(), equalTo( "Adam" ) );
    }
}
