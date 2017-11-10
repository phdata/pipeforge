# JDBC Metadata Parser

## Compiling the Project

In the project root dir

```
$ sbt assembly
```

This will create an uber jar that is used by the `pipeforge` command.
The folder `bin` can then be added to your PATH
## Configuration
The application takes no parametes and runs only off of configuration files.
Configuration templates can be found in ./conf.

pipewrench.conf: configuration for generation of `tables.yml`
source-database.conf: configuration for connecting to RDBMS database

Conf files must be placed in the directory the application is run from

## Generating Pipewrench Config

First compile the project
```
$ sbt assembly
````

Then run it with the helper script

```
$ ./build-pipewrench-config
```

## Testing

Tests require Docker.

Start the test Docker Oracle image
```
$ ./run-oracle-docker
```

Run the tests with SBT

```
$ sbt test
```
