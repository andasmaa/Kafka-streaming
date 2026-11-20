FROM apache/hive:3.1.3
# add the PostgreSQL JDBC driver so the metastore can talk to Postgres
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/hive/lib/postgresql-42.7.4.jar
