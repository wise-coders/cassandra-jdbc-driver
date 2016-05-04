package com.dbschema;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.security.InvalidParameterException;
import java.util.*;

public class CassandraClientURI {

    private static final String PREFIX = "jdbc:cassandra://";
    private static final String UTF_8 = "UTF-8";

    private final List<String> hosts;
    private final String database;
    private final String collection;
    private final String uri;
    private String userName;
    private char[] password;

    public CassandraClientURI( String uri ){
        try {
            this.uri = uri;
            if (!uri.startsWith(PREFIX))
                throw new IllegalArgumentException("uri needs to start with " + PREFIX);

            uri = uri.substring(PREFIX.length());

            String serverPart;
            String nsPart;
            String optionsPart;

            {
                int idx = uri.lastIndexOf("/");
                if (idx < 0) {
                    if (uri.contains("?")) {
                        throw new IllegalArgumentException("URI contains options without trailing slash");
                    }
                    serverPart = uri;
                    nsPart = null;
                    optionsPart = "";
                } else {
                    serverPart = uri.substring(0, idx);
                    nsPart = uri.substring(idx + 1);

                    idx = nsPart.indexOf("?");
                    if (idx >= 0) {
                        optionsPart = nsPart.substring(idx + 1);
                        nsPart = nsPart.substring(0, idx);
                    } else {
                        optionsPart = "";
                    }

                }
            }

            { // userName,password,hosts
                List<String> all = new LinkedList<String>();

                int idx = serverPart.indexOf("@");

                if (idx > 0) {
                    String authPart = serverPart.substring(0, idx);
                    serverPart = serverPart.substring(idx + 1);

                    idx = authPart.indexOf(":");
                    if (idx == -1) {
                        userName = URLDecoder.decode(authPart, UTF_8);
                    } else {
                        userName = URLDecoder.decode(authPart.substring(0, idx), UTF_8);
                        password = URLDecoder.decode(authPart.substring(idx + 1), UTF_8).toCharArray();
                    }
                }

                Collections.addAll(all, serverPart.split(","));

                hosts = Collections.unmodifiableList(all);
            }

            if (nsPart != null && nsPart.length() != 0) { // database,_collection
                int idx = nsPart.indexOf(".");
                if (idx < 0) {
                    database = nsPart;
                    collection = null;
                } else {
                    database = nsPart.substring(0, idx);
                    collection = nsPart.substring(idx + 1);
                }
            } else {
                database = null;
                collection = null;
            }

            Map<String, List<String>> optionsMap = parseOptions(optionsPart);
            warnOnUnsupportedOptions(optionsMap);
        } catch (UnsupportedEncodingException e) {
            throw new InvalidParameterException("This should not happen: " + e);
        }
    }

    public Cluster createBuilder() throws java.net.UnknownHostException{
        Cluster.Builder builder = Cluster.builder();
        int port = -1;
        for ( String host : hosts ){
            int idx = host.indexOf(":");
            if ( idx > 0 ){
                port = Integer.parseInt( host.substring( idx +1));
                host = host.substring( 0, idx );
            }
            builder.addContactPoints( InetAddress.getByName( host ) );
        }
        if ( port > -1 ){
            builder.withPort( port );

        }
        builder.withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(100L));
        if ( userName != null ){
            builder.withCredentials(userName, String.valueOf(password));
            System.out.println("Using authentication as user '" + userName + "'");
        }
        return builder.build();
    }



    static Set<String> allKeys = new HashSet<String>();

    static {
        allKeys.add("sample");
    }

    private void warnOnUnsupportedOptions(Map<String, List<String>> optionsMap) {
        for (String key : optionsMap.keySet()) {
            if (!allKeys.contains(key)) {
                System.out.println("Unknown or Unsupported Option '" + key + "'");
            }
        }
    }


    private String getLastValue(final Map<String, List<String>> optionsMap, final String key) {
        List<String> valueList = optionsMap.get(key);
        if (valueList == null) {
            return null;
        }
        return valueList.get(valueList.size() - 1);
    }

    private Map<String, List<String>> parseOptions(String optionsPart) {
        Map<String, List<String>> optionsMap = new HashMap<String, List<String>>();

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


    boolean _parseBoolean(String _in) {
        String in = _in.trim();
        return in != null && in.length() > 0 && (in.equals("1") || in.toLowerCase().equals("true") || in.toLowerCase()
                .equals("yes"));
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
    public char[] getPassword() {
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
     * Gets the database name
     *
     * @return the database name
     */
    public String getDatabase() {
        return database;
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
