/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2021 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.hdi.format.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.pentaho.hadoop.shim.HadoopShim;
import org.pentaho.hadoop.shim.ShimConfigsLoader;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.format.IOrcInputField;
import org.pentaho.hadoop.shim.api.format.IOrcMetaData;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcInputFormat;
import org.pentaho.hadoop.shim.common.ConfigurationProxy;
import org.pentaho.hadoop.shim.common.format.HadoopFormatBase;
import org.pentaho.hadoop.shim.common.format.orc.OrcMetaDataReader;
import org.pentaho.hadoop.shim.common.format.orc.OrcSchemaConverter;
import org.pentaho.hadoop.shim.common.format.orc.PentahoOrcInputFormat;

import java.io.InputStream;
import java.util.List;
import java.util.function.BiConsumer;

import static java.util.Objects.requireNonNull;

public class HDIOrcInputFormat extends PentahoOrcInputFormat {
  private static final String NOT_NULL_MSG = "filename and inputfields must not be null";
  private final Configuration conf;
  private final org.pentaho.hadoop.shim.api.internal.Configuration pentahoConf;
  private String fileName;
  private final HadoopShim shim;

  public HDIOrcInputFormat( NamedCluster namedCluster ) {
    super(namedCluster);
    shim = new HadoopShim();
    pentahoConf = shim.createConfiguration( namedCluster );

    if ( namedCluster == null ) {
      conf = new ConfigurationProxy();
    } else {
      conf = inClassloader( () -> {
        Configuration confProxy = new ConfigurationProxy();
        confProxy.addResource( "hive-site.xml" );
        BiConsumer<InputStream, String> consumer = confProxy::addResource;
        ShimConfigsLoader.addConfigsAsResources( namedCluster, consumer );
        return confProxy;
      } );
    }
  }

  @Override
  public IPentahoRecordReader createRecordReader( IPentahoInputSplit split ) {
    requireNonNull( fileName, NOT_NULL_MSG );
    requireNonNull( inputFields, NOT_NULL_MSG );
    return inClassloader( () -> new HDIOrcRecordReader( fileName, conf, inputFields, shim, pentahoConf ) );
  }

  @Override
  public List<IOrcInputField> readSchema() {
    return inClassloader( () -> readSchema(
            HDIOrcRecordReader.getReader(
                    requireNonNull( fileName, NOT_NULL_MSG ), conf, shim, pentahoConf ) ) );
  }

  @Override
  public void setInputFile( String fileName ) {
    this.fileName = fileName;
  }


}
