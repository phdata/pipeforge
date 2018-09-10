# Pipeforge

## Purpose
Pipeforge uses JDBC metadata to build the tables.yml file used with Pipewrench.


## CLI Args

- `configuration`: Builds Pipewrench `tables.yml` and `environment.yml` files from JDBC Metadata.  Writes the results to the configured output directory
    - `-e, --environment`: Path to source environment.yml file
    - `-p, --password`: Optional. Source database user password, user will be prompted to enter password if it is not supplied
    - `-t, --tables`: Optional.  Existing tables.yml file path.  A merge operation will be performed newly parsed tables will be appended to the original table definitions. 
- `merge`: Executes Pipewrench merge and stores the Pipewrench output to the specified directory.
    - `-d, --directory`: Pipewrench configuration directory containing both the Pipewrench `environment.yml` and `tables.yml`
    - `-t, --template`: Pipewrench template name
- `rest-api`: Starts a process for interacting with Pipeforge via REST and Yaml documents
    - `-p, --port`: Port to expose the pipewrench endpoints on
- `validate-schema`: Validated source RDBMS against Impala database.  `application.conf` properties `impala.hostname` and `impala.port` are required for this functionality to work.
    - `-e, --environment`: Path to source environment.yml file
    - `-p, --database-password`: Optional. Source database user password, user will be prompted to enter password if it is not supplied
    - `-h, --hadoop-password`: Optional. Hadoop user password, user will be prompted to enter password if it is not supplied

### Configuration Files

#### Application Configuration `application.conf`
[application.conf Example](src/main/resources/application.conf)

To provide a custom application

```
pipewrench { 
  virtualInstall = true # Determines whether Pipeforge should clone Pipewrench and setup virtual environment for Python.  Set to false if Pipewrench is already installed.
  git {
    url = "https://github.com/Cargill/pipewrench" # Git location of the pipewrench version to install.
  }
  directory {
    # Directory path containing installtion scripts `requirements.sh` and `generate-scripts.sh`.  Set to `conf` when using a packaged deployment.
    # When delopying pipeforge via bundled zip file this value should be set to `conf/`
    install = "src/main/resources"
    # Pipewrench installation location.
    pipewrench = "<fully qualified path where Pipewrench should be installed>"
    # Fully qualified path location of Pipewrench templates directory.
    templates = "<pipewrench template directory>"
    # Path where to write pipewrench configuration files and pipewrench output scripts 
    ingest = "." 
  }
}
impala {
  # Environment specific impala shell command.  Note -f is required as pipewrench passes sql files to the impala shell commands
  cmd = "<impala-shell command> -f "
  # OPTIONAL: Hostname of Impala daemon FQDN or load balanced url this value is used when doing schema validation between a source system and existing impala database
  hostname = "<Impala daemon FQDN>"
  # OPTIONAL: Port for JDBC communication to Impala daemon this value is used when doing schema validation between a source system and existing impala daemon
  port = <Impala daemon port>
}
# OPTIONAL: Used to query the Hive Metastore for column level comments, if left blank Impala comments will not be parsed
hive {
  metastore {
    # OPTIONAL: JDBC url for Hive Metastore ex. jdbc:mysql://<host>:<port>/<schema>
    url = "<hive metastore jdbc url>"
    # OPTIONAL: Hive metastore schema
    schema = "<hive metasotre schema>"
    # OPTIONAL: Hive metastore read only user
    username = "<hive metastore username>"
    # OPTIONAL: Hive metastore read only user password
    password = "<hive metastore password>"
    # OPTIONAL: MySQL is the only support option at this time
    databaseType = "mysql"
  }
}
```

#### Source Environment `environment.yml`
[Yaml Example](src/main/resources/environment.yml)
```yaml
name: dev.employee # Unique name for data ingestion
group: edh_dev_employee # Associated AD group for ingestion
databaseType: mysql # Database type must be mysql, oracle, mssql, hana, or teradata, as400, redshift, impala
schema: employees # RDMS or impala database / schema
jdbcUrl: "jdbc:mysql://localhost:3306/employees" # JDBC Url for connecting to database. Impala example: jdbc:hive2://<host>:21050/<database>;AuthMech=3;ssl=true
username: employee # RDMS username or AD user / service account user name (Impala)
objectType: table # Database object type to be ingested must be either table or view
# tables: # Optionally add a whitelisting of tables to ingest
#   - employee
metadata: # Metadata map to be applied to every table's tblproperties. https://www.cloudera.com/documentation/enterprise/latest/topics/impala_create_table.html
  SOURCE: employee database # Source database identifier
  LOAD_FREQUENCY: Daily # Frequency of data ingestion
  CONTACT: developer@phdata.io # Distribution list for data owners
hadoopUser: ps_dev_employee # Hadoop user recommended to use a process account
passwordFile: hdfs:///user/developer/.employee_db_password # Location of sqoop's password file recommended HDFS location
stagingDatabase:
  name: employee
  path: hdfs:///ns/user/developer/staging/db
rawDatabase:
  name: employee_raw
  path: hdfs:///ns/user/developer/raw/db
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
$INSTALL_LOCATION/bin/pipeforge configuration \
  -e <path to pipeforge environment file>
```

### Pipeforge Rest Api
```
$INSTALL_LOCATION/bin/pipeforge rest-api \
  -p <api port> -Dconfig.file=<path to application.conf>
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
