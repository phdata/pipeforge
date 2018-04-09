/*
 * Copyright 2018 phData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.phdata.pipeforge.rest

import io.phdata.pipeforge.rest.module.RestModule
import io.phdata.pipewrench.PipewrenchService

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * RestApp an application exposing Pipeforge Rest endpoints
 *
 * @param service PipewrenchService
 */
class RestApp(service: PipewrenchService) extends RestModule {

  override def pipewrenchService: PipewrenchService = service

  /**
   * Starts the rest api
   * @param port Port to run the webservice on
   */
  def start(port: Int) = Await.ready(restApi.start(port), Duration.Inf)

}
