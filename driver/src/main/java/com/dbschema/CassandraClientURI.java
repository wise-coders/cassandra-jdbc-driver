package com.dbschema;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;

import java.net.InetAddress;
import java.util.*;

public class CassandraClientURI {

    static final String PREFIX = "jdbc:cassandra://";

    private final List<String> hosts;
    private final String keyspace;
    private final String collection;
    private final String uri;
    private final String userName;
    private final String password;

    public CassandraClientURI(String uri, Properties info) {
        this.uri = uri;
        if (!uri.startsWith(PREFIX))
            throw new IllegalArgumentException("URI needs to start with " + PREFIX);

        uri = uri.substring(PREFIX.length());


        String serverPart;
        String nsPart;
        Map<String, List<String>> options = null;

        {
            int lastSlashIndex = uri.lastIndexOf("/");
            if (lastSlashIndex < 0) {
                if (uri.contains("?")) {
                    throw new IllegalArgumentException("URI contains options without trailing slash");
                }
                serverPart = uri;
                nsPart = null;
            } else {
                serverPart = uri.substring(0, lastSlashIndex);
                nsPart = uri.substring(lastSlashIndex + 1);

                int questionMarkIndex = nsPart.indexOf("?");
                if (questionMarkIndex >= 0) {
                    options = parseOptions(nsPart.substring(questionMarkIndex + 1));
                    nsPart = nsPart.substring(0, questionMarkIndex);
                }
            }
        }

        this.userName = getOption(info, options, "user");
        this.password = getOption(info, options, "password");

        { // userName,password,hosts
            List<String> all = new LinkedList<String>();

            Collections.addAll(all, serverPart.split(","));

            hosts = Collections.unmodifiableList(all);
        }

        if (nsPart != null && nsPart.length() != 0) { // keyspace._collection
            int dotIndex = nsPart.indexOf(".");
            if (dotIndex < 0) {
                keyspace = nsPart;
                collection = null;
            } else {
                keyspace = nsPart.substring(0, dotIndex);
                collection = nsPart.substring(dotIndex + 1);
            }
        } else {
            keyspace = null;
            collection = null;
        }
    }

    /**
     * @return option from properties or from uri if it is not found in properties.
     * null if options was not found.
     */
    private String getOption(Properties properties, Map<String, List<String>> options, String optionName) {
        if (properties != null) {
            String option = (String) properties.get(optionName);
            if (option != null) {
                return option;
            }
        }
        return getLastValue(options, optionName);
    }

    Cluster createCluster() throws java.net.UnknownHostException {
        Cluster.Builder builder = Cluster.builder();
        int port = -1;
        for ( String host : hosts ){
            int idx = host.indexOf(":");
            if ( idx > 0 ){
                port = Integer.parseInt( host.substring( idx +1).trim() );
                host = host.substring( 0, idx ).trim();
            }
            builder.addContactPoints( InetAddress.getByName( host ) );
        }
        if ( port > -1 ){
            builder.withPort( port );

        }
        builder.withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(100L));
        if ( userName != null ){
            builder.withCredentials(userName, password);
            System.out.println("Using authentication as user '" + userName + "'");
        }
        return builder.build();
    }


    private String getLastValue(final Map<String, List<String>> optionsMap, final String key) {
        if (optionsMap == null) return null;
        List<String> valueList = optionsMap.get(key);
        if (valueList == null || valueList.size() == 0) return null;
        return valueList.get(valueList.size() - 1);
    }

    private Map<String, List<String>> parseOptions(String optionsPart) {
        Map<String, List<String>> optionsMap = new HashMap<>();

        for (String _part : optionsPart.split("&|;")) {
            int idx = _part.indexOf("=");
            if (idx >= 0) {
                String key = _part.substring(0, idx).toLowerCase();
                String value = _part.substring(idx + 1);
                List<String> valueList = optionsMap.get(key);
                if (valueList == null) {
                    valueList = new ArrayList<String>(1);
                }
                valueList.add(value);
                optionsMap.put(key, valueList);
            }
        }

        return optionsMap;
    }


    // ---------------------------------

    /**
     * Gets the username
     *
     * @return the username
     */
    public String getUsername() {
        return userName;
    }

    /**
     * Gets the password
     *
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Gets the list of hosts
     *
     * @return the host list
     */
    public List<String> getHosts() {
        return hosts;
    }

    /**
     * Gets the keyspace name
     *
     * @return the keyspace name
     */
    public String getKeyspace() {
        return keyspace;
    }


    /**
     * Gets the collection name
     *
     * @return the collection name
     */
    public String getCollection() {
        return collection;
    }

    /**
     * Get the unparsed URI.
     *
     * @return the URI
     */
    public String getURI() {
        return uri;
    }



    @Override
    public String toString() {
        return uri;
    }
}
