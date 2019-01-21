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
package org.neo4j.driver.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.util.function.Supplier;

import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.StatementResult;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.util.CertificateToolUtil.CertificateKeyPair;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.Config.TrustStrategy.trustCustomCertificateSignedBy;
import static org.neo4j.driver.util.CertificateToolUtil.createNewCertificateAndKey;
import static org.neo4j.driver.util.CertificateToolUtil.createNewCertificateAndKeySignedBy;

@ParallelizableIT
public class TrustCustomCertificateIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @Test
    void shouldAcceptServerWithCertificateSignedByDriverCertificate() throws Throwable
    {
        // Given root certificate
        CertificateKeyPair<File,File> root = createNewCertificateAndKey();

        // When
        CertificateKeyPair<File,File> server = createNewCertificateAndKeySignedBy( root );
        neo4j.updateEncryptionKeyAndCert( server.key(), server.cert() );

        // Then
        shouldBeAbleToRunCypher( () -> createDriverWithCustomCertificate( root.cert() ) );
    }

    @Test
    void shouldAcceptServerWithSameCertificate() throws Throwable
    {
        shouldBeAbleToRunCypher( () -> createDriverWithCustomCertificate( neo4j.tlsCertFile() ) );
    }

    @Test
    void shouldRejectServerWithUntrustedCertificate() throws Throwable
    {
        // Given a driver with a (random) cert
        CertificateKeyPair<File,File> certificateAndKey = createNewCertificateAndKey();

        // When & Then
        SecurityException error = assertThrows( SecurityException.class, () -> createDriverWithCustomCertificate( certificateAndKey.cert() ) );
    }

    private void shouldBeAbleToRunCypher( Supplier<Driver> driverSupplier )
    {
        try ( Driver driver = driverSupplier.get(); Session session = driver.session() )
        {
            StatementResult result = session.run( "RETURN 1 as n" );
            assertThat( result.single().get( "n" ).asInt(), equalTo( 1 ) );
        }
    }

    private Driver createDriverWithCustomCertificate( File cert )
    {
        return GraphDatabase.driver( neo4j.uri(), neo4j.authToken(),
                Config.builder().withEncryption().withTrustStrategy( trustCustomCertificateSignedBy( cert ) ).build() );
    }
}
