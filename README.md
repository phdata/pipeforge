# Pipeforge

## Purpose
Pipeforge uses JDBC metadata to build the tables.yml file used with Pipewrench.

## CLI Arguments
- -s | --database-configuration: Path to source database configuration file
- -p | --database-password: Source database user password
- -m | --table-metadata: Yaml file containing additional metadata that should be applied to each table
- -o | --output-path: Output path where the tables.yml file should be written
- -c | --skip-whitelist-check: Optional. Pipeforge checks table names by default to make sure they exist in the source schema. Use the -c option to skip the check.

### Source Database Configuration File
```hocon
database-type="mysql" // REQUIRED Source database type valid options are mssql, mysql, and oracle
jdbc-url="jdbc:mysql://localhost:3306/employees" // REQUIRED JDBC connection url
schema="employees" // REQUIRED Schema or database name to read tables or views from
username="employee" // REQUIRED Source database username
object-type="table" // REQUIRED Determines whether to parse tables or views valid options are table or view
tables=["table1", "table2"] // OPTIONAL Table whitelist, only these tables will be parsed
```

### Table Metadata File
```yaml
META_LOAD_FREQUENCY: "DAILY"
# Any additional metadata properties
```

## Build

### Creating a Jar

```sbtshell
sbt clean assembly
```

Creates a jar with the required dependencies ([sbt-assembly](https://github.com/sbt/sbt-assembly)).  The jar is found in  `target/scala-2.12/pipeforce_2.12-<version>.jar`.

1. Copy jar to `$INSTALL_LOCATION`
2. Change directory to `$INSTALL_LOCATION`
3. Execute 
```
java -cp pipforge-<version>.jar io.phdata.jdbc.PipewrenchConfigBuilder \
    -s <database configuration file> \
    -p <database password> \
    -m <table metadata> \
    -o <output path>
```

### Distributing application

```sbtshell
sbt clean assembly universal:packageBin
```
Creates a bundled zip application with required dependencies and executable scripts ([sbt-native-packager](https://github.com/sbt/sbt-native-packager)).  Package is found in `target/universal/pipeforge-<version>.zip`.

1. Copy zip file to `$INSTALL_LCOATION`
2. Change directory to `$INSTALL_LOCATION`
3. Execute `unzip pipforge-<version>.zip`
4. Change directory into `pipeforge-<version>`
5. Execute 
```
bin/pipeforge \
    -s <database configuration file>
    -p <database password>
    -m <table metadata>
    -o <output path> 
```
# Testing

## Unit tests

```sbtshell
sbt clean test
```

## Integration Tests

```sbtshell
sbt clean it:test
```

NOTE: [Docker](https://www.docker.com/) must be [installed](https://docs.docker.com/engine/installation/) on the system running integration tests.  Docker is used to spin up test databases during testing.
