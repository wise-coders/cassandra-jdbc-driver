# Cassandra JDBC Driver

This is an open source JDBC Driver for Cassandra.
The driver is used by [DbSchema](http://www.dbschema.com) Database Designer - an interactive diagram designer with relational data explorer, visual query builder, random data generator, forms and reports and many other tools.

## Download Binary

[Available here](http://www.dbschema.com/jdbc-drivers/CassandraJdbcDriver.zip). Unpack and include all jars in your classpath. 
You can test the driver by simply downloading [DbSchema Database Designer](http://www.dbschema.com). The driver is included in the software package.

## Description

Driver connectivity works on top of the native Cassandra Java driver.

```
#!java


import java.sql.Connection;
import java.sql.PreparedStatement;

...

Class.forName("com.dbschema.CassandraJdbcDriver");
Properties properties = new Properties();
properties.put("user", "someuser");
properties.put("password", "somepassword" );
Connection con = DriverManager.getConnection("jdbc:cassandra://host1:9160/keyspace1", properties);
// OTHER URL: jdbc:cassandra://host1--host2--host3:9160/keyspace1?primarydc=DC1&backupdc=DC2&consistency=QUORUM
String query = "UPDATE Test SET a=? WHERE KEY=?";
PreparedStatement statement = con.prepareStatement(query);

statement.setLong(1, 100);
statement.setString(2, "key0");

statement.executeUpdate();

statement.close();
```


The driver implements a PreparedStatement where native Cassandra queries can be passed.
The result will be returned as ResultSet with one column containing the Map JSon object.

Any contributions to this project are welcome.
We are looking forward to improve this and make possible to execute all Cassandra native queries via JDBC.