# Cassandra JDBC Driver

This is an open source JDBC Driver for Cassandra.
The driver is provided and used by [DbSchema Database Designer](http://www.dbschema.com) - an interactive Cassandra client and designer tool.
You can test this driver by downloading the DbSchema trial version.

## Downloading the Driver Binaries

[Available here](http://www.dbschema.com/jdbc-drivers/CassandraJdbcDriver.zip). Unpack and include all jars in your classpath. 

The driver can be tested by downloading and installing [DbSchema](http://www.dbschema.com). The tool has a 15 days trial period.
In DbSchema Connection Dialog 'Use URI' you can set an URL as below, with different connection parameters.
DbSchema will download the driver in the local user home folder .DbSchema/drivers/Cassandra ( C:\Users\YourUser\.DbSchema\drivers\Cassandra ).

## Technical Support

Please create an issue or write [here](http://www.dbschema.com/support.php) for any question or issue with the driver.

## How to Configure the JDBC Driver

* Java Driver Class: com.dbschema.CassandraJdbcDriver
* JDBC URL: jdbc:cassandra://host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[keyspace][?options]]
* Website: [DbSchema](https://www.dbschema.com/cassandra-jdbc-driver.html)

The driver we wrote on top of the native [Cassandra Java Driver](https://github.com/datastax/java-driver)

## Connecting using SSL

For this set this URL parameters:

javax.net.ssl.trustStore=/path/to/client.truststore&javax.net.ssl.trustStorePassword=password123&amp;

If you're using client authentication:

javax.net.ssl.keyStore=/path/to/client.keystore&javax.net.ssl.keyStorePassword=password123

This parameters can be also set in DbSchema.vmoptions file like :

-Djavax.net.ssl.trustStore=/path/to/client.truststore

-Djavax.net.ssl.trustStorePassword=password123

If you're using client authentication:

-Djavax.net.ssl.keyStore=/path/to/client.keystore

-Djavax.net.ssl.keyStorePassword=password123


