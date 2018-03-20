# Pipeforge Rest Api

## Purpose
Exposes services for interacting with Pipeforge through rest endpoints.

## Requirements

### Installation
[Pipewrench](http://github.com/Cargill/pipewrench) must be installed on the host running the rest service.

### Configuration
Pipewrench Environment and Configuration files are written to a [configured](src/main/resources/application.conf) directory.  This directory should contain a copy of Pipewrench templates and the [generate-scripts.sh](src/main/resources/generate-scripts.sh) file.

## Endpoints
[Postman File](src/main/resources/pipeforge-api.postman_collection.json)

### /pipeforge
#### GET
Basic text response endpoint.
#### POST
Executes all steps in producing Pipewrench environment, configuration and output files.
##### Parameters
- password: Database password
##### Output
- A tables.yml is written to `{pipewrench.directory.output}/{group}/{name}/tables.yml`
- A env.yml is written to `{pipewrench.directory.output}/{group}/{name}/env.yml`
- A pipewrench output directory is written to `{pipewrench.directory.output}/{group}/{name}/output`

### /pipewrench
#### GET
Basic text response endpoint.

### /pipewrench/merge
#### POST
Executes `pipewrench-merge`
##### Parameters
- group: Name of the group specified in the Pipeforge environment file
- name: Ingest name specified in the Pipeforge environment file
- template: Pipewrench template to be used when executing the merge
##### Output
- A pipewrench output directory is written to `{pipewrench.directory.output}/{group}/{name}/output`

### /pipewrench/configuration
#### POST
Saves a Pipewrench Configuration file to the configured directory.
##### Body
Pipewrench Configuration json document with column comments populated.  Output of PUT /pipewrench/configuration.
##### Output
- A tables.yml is written to `{pipewrench.directory.output}/{group}/{name}/tables.yml`
#### PUT
Produces a [Pipewrench Configuration](../pipewrench/src/main/scala/io/phdata/pipewrench/domain/Configuration.scala) from database metadata.
##### Body
[Pipeforge Environment](src/main/scala/io/phdata/pipeforge/rest/domain/Environment.scala) json [Example](src/main/resources/env.json)

### /pipewrench/environment
#### POST
Saves a [Pipewrench Environment](../pipewrench/src/main/scala/io/phdata/pipewrench/domain/Environment.scala) file to the configured directory.
##### Body
[Pipeforge Environment](src/main/scala/io/phdata/pipeforge/rest/domain/Environment.scala) json [Example](src/main/resources/env.json)