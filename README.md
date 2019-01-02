# Cassandra JDBC Driver

Based on [DbSchema Cassandra driver](https://bitbucket.org/dbschema/cassandra-jdbc-driver/src/master/)

## How to build jar
```
# Linux, MacOs
./gradlew jar

# Windows
gradlew.bat jar
```

You'll find it in build/libs

# SSL
Set property `sslenabled=true`

Pass arguments for your keystore and trust store. 
```
-Djavax.net.ssl.trustStore=/path/to/client.truststore
-Djavax.net.ssl.trustStorePassword=password123
# If you're using client authentication:
-Djavax.net.ssl.keyStore=/path/to/client.keystore
-Djavax.net.ssl.keyStorePassword=password123
```