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

package io.phdata.pipeforge.rest.domain

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import io.phdata.pipewrench.domain.{ Column, Configuration, Kudu, Table }
import spray.json.DefaultJsonProtocol

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit def environmentJsonFormat = jsonFormat13(Environment)
  implicit def statusJsonFormat      = jsonFormat3(Status)

  implicit def columnJsonFormat        = jsonFormat5(Column)
  implicit def kuduJsonFormat          = jsonFormat2(Kudu)
  implicit def tableJsonFormat         = jsonFormat8(Table)
  implicit def configurationJsonFormat = jsonFormat11(Configuration)
}
