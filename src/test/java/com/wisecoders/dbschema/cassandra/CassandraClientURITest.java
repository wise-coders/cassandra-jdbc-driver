package com.wisecoders.dbschema.cassandra;

import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Copyright Wise Coders GmbH. The Cassandra JDBC driver is build to be used with DbSchema Database Designer https://dbschema.com
 * Free to use by everyone, code modifications allowed only to
 * the public repository https://github.com/wise-coders/cassandra-jdbc-driver
 */

public class CassandraClientURITest {

    @Test(expected = IllegalArgumentException.class)
    public void testUriWithInvalidParameters() {
        new CassandraClientURI("jdbc:cassandra://localhost:9042?name=cassandra", null);
    }

    @Test
    public void testSimpleUri() {
        CassandraClientURI uri = new CassandraClientURI("jdbc:cassandra://localhost:9042", null);
        List<String> hosts = uri.getHosts();
        assertEquals(1, hosts.size());
        assertEquals("localhost:9042", hosts.get(0));
    }

    @Test
    public void testUriWithUserName() {
        CassandraClientURI uri = new CassandraClientURI("jdbc:cassandra://localhost:9042/?user=cassandra", null);
        List<String> hosts = uri.getHosts();
        assertEquals(1, hosts.size());
        assertEquals("localhost:9042", hosts.get(0));
        assertEquals("cassandra", uri.getUsername());
    }

    @Test
    public void testOptionsInProperties() {
        Properties properties = new Properties();
        properties.put("user", "NameFromProperties");
        properties.put("password", "PasswordFromProperties");
        CassandraClientURI uri = new CassandraClientURI(
                "jdbc:cassandra://localhost:9042/?user=cassandra&password=cassandra",
                properties);
        List<String> hosts = uri.getHosts();
        assertEquals(1, hosts.size());
        assertEquals("localhost:9042", hosts.get(0));
        assertEquals("NameFromProperties", uri.getUsername());
        assertEquals("PasswordFromProperties", uri.getPassword());
    }


    @Test
    public void testSslEnabledOptionTrue() throws GeneralSecurityException, IOException {
        Properties properties = new Properties();
        properties.put("sslenabled", "true");
        CassandraClientURI uri = new CassandraClientURI(
                "jdbc:cassandra://localhost:9042/?name=cassandra&password=cassandra",
                properties);
        assertNotNull(uri.getSslContext());
        assertTrue(uri.getSslEnabled());
    }

    @Test
    public void testSslEnabledOptionFalse() {
        Properties properties = new Properties();
        properties.put("sslenabled", "false");
        CassandraClientURI uri = new CassandraClientURI(
                "jdbc:cassandra://localhost:9042/?name=cassandra&password=cassandra",
                properties);
        assertFalse(uri.getSslEnabled());
    }

    @Test
    public void testNullSslEnabledOptionFalse() {
        Properties properties = new Properties();
        CassandraClientURI uri = new CassandraClientURI(
                "jdbc:cassandra://localhost:9042/?name=cassandra&password=cassandra",
                properties);
        assertFalse(uri.getSslEnabled());
    }

    @Test
    public void testAwsSecretNotFound() {
        SecretsManagerException sme = (SecretsManagerException) SecretsManagerException
                .builder()
                .message("Any error")
                .build();

        String region = "sa-east-1";
        String secretName = "a_secret_name";

        try (MockedStatic<AWSUtil> utilities = Mockito.mockStatic(AWSUtil.class)) {
            utilities.when(() -> AWSUtil.getSecretValue(region, secretName)).thenThrow(sme);

            try {
                CassandraClientURI uri = new CassandraClientURI(
                        "jdbc:cassandra://localhost:9042/?name=cassandra&password=cassandra&awsregion=sa-east-1&awssecretname=a_secret_name&anyFieldEndingWithPassword=another_pass",
                        new Properties());
            } catch (Exception e) {
                assertTrue(e instanceof SecretsManagerException);
            }
        }


    }
}