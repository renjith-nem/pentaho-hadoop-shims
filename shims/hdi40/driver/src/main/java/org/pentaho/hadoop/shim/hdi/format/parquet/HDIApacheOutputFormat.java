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
package org.pentaho.hadoop.shim.hdi.format.parquet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.pentaho.hadoop.shim.HadoopShim;
import org.pentaho.hadoop.shim.ShimConfigsLoader;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.common.ConfigurationProxy;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoApacheOutputFormat;

import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.util.function.BiConsumer;

public class HDIApacheOutputFormat extends PentahoApacheOutputFormat {

  private HadoopShim shim;
  private org.pentaho.hadoop.shim.api.internal.Configuration pentahoConf;
  private final NamedCluster namedCluster;


  @SuppressWarnings( "squid:S1874" )
  public HDIApacheOutputFormat( NamedCluster namedCluster ) {
    logger.info( "We are initializing parquet output format" );
    this.namedCluster = namedCluster;
    inClassloader( () -> {
      ConfigurationProxy conf = new ConfigurationProxy();
      shim = new HadoopShim();
      if ( namedCluster != null ) {
        // if named cluster is not defined, no need to add cluster resource configs
        pentahoConf = shim.createConfiguration( this.namedCluster );
        BiConsumer<InputStream, String> consumer = conf::addResource;
        ShimConfigsLoader.addConfigsAsResources( namedCluster, consumer );
      }

      job = Job.getInstance( conf );

      job.getConfiguration().set( ParquetOutputFormat.ENABLE_JOB_SUMMARY, "false" );
      ParquetOutputFormat.setEnableDictionary( job, false );
    } );
  }

  @Override
  public void setOutputFile( String file, boolean override ) throws Exception {
    inClassloader( () -> {
      outputFile = new Path( file );
      FileSystem fs = (FileSystem) shim.getFileSystem( pentahoConf ).getDelegate();
      if ( fs.exists( outputFile ) ) {
        if ( override ) {
          fs.delete( outputFile, true );
        } else {
          throw new FileAlreadyExistsException( file );
        }
      }
      this.outputFile = new Path( fs.getUri().toString() + this.outputFile.toUri().getPath() );
      this.outputFile = fs.makeQualified( this.outputFile );
      this.job.getConfiguration().set( "mapreduce.output.fileoutputformat.outputdir", this.outputFile.toString() );
    } );
  }

  public String generateAlias( String pvfsPath ) {
    return null;
  }
}
