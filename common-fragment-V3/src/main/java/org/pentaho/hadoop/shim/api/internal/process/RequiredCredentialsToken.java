/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.pentaho.hadoop.shim.api.internal.process;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * User: Dzmitry Stsiapanau Date: 01/28/2016 Time: 15:53
 */

@Documented
@Inherited
@Target( ElementType.TYPE )
@Retention( RetentionPolicy.RUNTIME )
public @interface RequiredCredentialsToken {

  String CREDENTIALS_REQUIRED_PROPERTY_NAME = "pentaho.mapreduce.required.token.types";

  enum Type {
    HBASE, HIVE, YARN, OOZIE, HDFS
  }

  Type[] value();
}
