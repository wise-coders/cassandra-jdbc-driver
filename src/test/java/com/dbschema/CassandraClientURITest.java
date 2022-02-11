package com.dbschema;

import org.junit.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

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
    public void testSslEnabledOptionTrue() {
        Properties properties = new Properties();
        properties.put("sslenabled", "true");
        CassandraClientURI uri = new CassandraClientURI(
                "jdbc:cassandra://localhost:9042/?name=cassandra&password=cassandra",
                properties);
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
}