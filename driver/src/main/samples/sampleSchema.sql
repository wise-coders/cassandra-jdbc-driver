CREATE TABLE radacct (
 acctuniqueid text,
  acctsessionid text,
  username text,
  groupname text,
  realm text,
  nasipaddress text,
  nasportid text,
  nasporttype text,
  acctstarttime timestamp,
  acctupdatetime timestamp,
  acctstoptime timestamp,
  acctauthentic text,
  connectinfo_start text,
  connectinfo_stop text,
  acctinputoctets bigint,
  acctoutputoctets bigint,
  calledstationid text,
  callingstationid text,
  servicetype text,
  terminatecause text,
  framedprotocol text,
  framedipaddress text,
  PRIMARY KEY (acctuniqueid)
);

CREATE INDEX ON radacct(username);
CREATE INDEX ON radacct(framedipaddress);
CREATE INDEX ON radacct(nasipaddress);

CREATE TABLE radnasreboot (
  nasipaddress text,
  timestamp bigint,
  PRIMARY KEY (timestamp, nasipaddress)
);

CREATE TABLE radpostauth (
  username text,
  pass text,
  reply text,
  authdate timestamp,
  PRIMARY KEY (username, authdate)
) WITH CLUSTERING ORDER BY (authdate ASC);

CREATE TABLE radcheck (
  id uuid,
  username text,
  attribute text,
  op text,
  value text,
  PRIMARY KEY (username, attribute)
);

CREATE TABLE radreply (
  id uuid,
  username text,
  attribute text,
  op text,
  value text,
  PRIMARY KEY (username, attribute)
);

CREATE TABLE radgroupcheck (
  id uuid,
  groupname text,
  attribute text,
  op text,
  value text,
  PRIMARY KEY (groupname, attribute)
);

CREATE TABLE radgroupreply (
  id uuid,
  groupname text,
  attribute text,
  op text,
  value text,
  PRIMARY KEY (groupname, attribute)
);

CREATE TABLE radusergroup (
  username text,
  priority int,
  groupname text,
  PRIMARY KEY (username, priority)
) WITH CLUSTERING ORDER BY (priority ASC);

CREATE TABLE nas (
  id uuid PRIMARY KEY,
  nasname text,
  shortname text,
  type text,
  ports int,
  secret text,
  server text,
  community text,
  description text
);

CREATE INDEX ON nas(nasname);