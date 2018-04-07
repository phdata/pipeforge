# Pipeforge Rest Api

## Purpose
Exposes services for interacting with Pipeforge through rest endpoints.

## Requirements

### Installation
[Pipewrench](http://github.com/Cargill/pipewrench) must be installed on the host running the rest service.

### Configuration
Pipewrench Environment and Configuration files are written to a [configured](../src/main/resources/application.conf) directory.  This directory should contain a copy of Pipewrench templates and the [generate-scripts.sh](src/main/resources/generate-scripts.sh) file.

## Endpoints
[Postman File](src/main/resources/pipeforge-api.postman_collection.json)

### /pipewrench
#### GET
Basic text response endpoint.

### /pipewrench/merge
#### POST
Executes `pipewrench-merge`
##### Parameters
- template: Pipewrench template to be used when executing the merge
##### Body
A complete [Pipewrench Configuration](../pipewrench/src/main/scala/io/phdata/pipewrench/domain/Configuration.scala)
##### Output
- A pipewrench output directory is written to `{pipewrench.directory.output}/{group}/{name}/output`

### /pipewrench/configuration
#### PUT
Produces a [Pipewrench Configuration](../pipewrench/src/main/scala/io/phdata/pipewrench/domain/Configuration.scala) from database metadata.
##### Body
[Pipeforge Environment](src/main/scala/io/phdata/pipeforge/rest/domain/Environment.scala) json [Example](src/main/resources/env.json)

### /pipewrench/environment
#### POST
Saves a [Pipewrench Environment](../pipewrench/src/main/scala/io/phdata/pipewrench/domain/Environment.scala) file to the configured directory.
##### Body
[Pipeforge Environment](src/main/scala/io/phdata/pipeforge/rest/domain/Environment.scala) json [Example](src/main/resources/env.json)