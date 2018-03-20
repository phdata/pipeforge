# Pipeforge

## Purpose
Pipeforge uses JDBC metadata to build the tables.yml file used with Pipewrench.

## CLI Arguments
- `-s, --database-configuration`: Path to source database configuration file
- `-p, --database-password`: Source database user password
- `-o, --output-path`: Output path where the tables.yml file should be written
- `-c, --skip-whitelist-check`: Optional. Pipeforge checks table names by default to make sure they exist in the source schema. Use the -c option to skip the check.

### Configuration Files

#### Source Database `env.yml`
[Yaml Example](src/main/resources/env.yml)
```yaml
name: dev.employee # Unique name for data ingestion
group: edh_dev_employee # Associated AD group for ingestion
databaseType: mysql # Database type must be mysql, oracle, mssql, hana, or teradata
schema: employees # Database schema to ingest
jdbcUrl: "jdbc:mysql://localhost:3306/employees" # JDBC Url for connecting to database
username: employee # Database user name
objectType: table # Database object type to be ingested must be either table or view
# tables: # Optionally add a whitelisting of tables to ingest
#   - employee
metadata: # Metadata map to be applied to every table's tblproperties. https://www.cloudera.com/documentation/enterprise/latest/topics/impala_create_table.html
  SOURCE: employee database # Source database identifier
  LOAD_FREQUENCY: Daily # Frequency of data ingestion
  CONTACT: developer@phdata.io # Distribution list for data owners
hdfsPath: "hdfs://ns/user/developer/db" # Base HDFS Path to write data to
hadoopUser: ps_dev_employee # Hadoop user recommended to use a process account
passwordFile: hdfs://user/developer/.employee_db_password # Location of sqoop's password file recommended HDFS location
destinationDatabase: dev_employees # Hadoop destination database
```

## Build

### Creating a Jar

```sbtshell
sbt clean assembly
```

Creates a jar with the required dependencies ([sbt-assembly](https://github.com/sbt/sbt-assembly)).  The jar is found in  `target/scala-2.12/pipeforce_2.12-<version>.jar`.

1. Copy jar to `$INSTALL_LOCATION`
2. Change directory to `$INSTALL_LOCATION`

### Distributing application

```sbtshell
sbt clean assembly universal:packageBin
```
Creates a bundled zip application with required dependencies and executable scripts ([sbt-native-packager](https://github.com/sbt/sbt-native-packager)).  Package is found in `target/universal/pipeforge-<version>.zip`.

1. Copy zip file to `$INSTALL_LCOATION`
2. Change directory to `$INSTALL_LOCATION`
3. Execute `unzip pipforge-<version>.zip`
4. Change directory into `pipeforge-<version>`

## Execution

### Pipewrench Configuration Builder
```
$INSTALL_LOCATION/bin/pipeforge pipewrench \
  -s <path to pipeforge environment file>
  -p <database password>
  -o <output path where pipwrench environment and configuration files should be written>
```

### Pipeforge Rest Api
```
$INSTALL_LOCATION/bin/pipeforge rest-api \
  -p <api port>
```
[Documentation](rest-api/README.md)

## Testing

## Unit tests

```sbtshell
sbt clean test
```

## Integration Tests

```sbtshell
sbt clean it:test
```

NOTE: [Docker](https://www.docker.com/) must be [installed](https://docs.docker.com/engine/installation/) on the system running integration tests.  Docker is used to spin up test databases during testing.
