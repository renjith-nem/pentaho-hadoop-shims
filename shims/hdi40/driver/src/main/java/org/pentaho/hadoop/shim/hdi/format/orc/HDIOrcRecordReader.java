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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.pentaho.hadoop.shim.HadoopShim;
import org.pentaho.hadoop.shim.api.format.IOrcInputField;
import org.pentaho.hadoop.shim.api.format.IOrcMetaData;
import org.pentaho.hadoop.shim.common.format.orc.OrcMetaDataReader;
import org.pentaho.hadoop.shim.common.format.orc.OrcSchemaConverter;
import org.pentaho.hadoop.shim.common.format.orc.PentahoOrcRecordReader;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HDIOrcRecordReader extends PentahoOrcRecordReader {

  HDIOrcRecordReader( String fileName, Configuration conf,
                      List<? extends IOrcInputField> dialogInputFields, HadoopShim shim,
                      org.pentaho.hadoop.shim.api.internal.Configuration pentahoConf ) {
    super( fileName,  conf, dialogInputFields);

    this.dialogInputFields = dialogInputFields;
    Reader reader = getReader( fileName, conf, shim, pentahoConf );
    try {
      recordReader = reader.rows();
    } catch ( IOException e ) {
      throw new IllegalArgumentException( "Unable to get record reader for file " + fileName, e );
    }
    typeDescription = reader.getSchema();
    OrcSchemaConverter orcSchemaConverter = new OrcSchemaConverter();
    orcInputFields = orcSchemaConverter.buildInputFields( typeDescription );
    IOrcMetaData.Reader orcMetaDataReader = new OrcMetaDataReader( reader );
    orcMetaDataReader.read( orcInputFields );
    batch = typeDescription.createRowBatch();

    //Create a map of orc fields to meta columns
    Map<String, Integer> orcColumnNumberMap = new HashMap<>();
    int orcFieldNumber = 0;
    for ( String orcFieldName : typeDescription.getFieldNames() ) {
      orcColumnNumberMap.put( orcFieldName, orcFieldNumber++ );
    }

    //Create a map of input fields to Orc Column numbers
    schemaToOrcSubcripts = new HashMap<>();
    for ( IOrcInputField inputField : dialogInputFields ) {
      if ( inputField != null ) {
        Integer colNumber = orcColumnNumberMap.get( inputField.getFormatFieldName() );
        if ( colNumber == null ) {
          throw new IllegalArgumentException(
                  "Column " + inputField.getFormatFieldName()
                          + " does not exist in the ORC file.  Please use the getFields button" );
        } else {
          schemaToOrcSubcripts.put( inputField.getPentahoFieldName(), colNumber );
        }
      }
    }

    try {
      setNextBatch();
    } catch ( IOException e ) {
      throw new IllegalArgumentException( "No rows to read in " + fileName, e );
    }
  }

  static Reader getReader( String fileName, Configuration conf, HadoopShim shim,
                           org.pentaho.hadoop.shim.api.internal.Configuration pentahoConf ) {

    try {

      Path filePath = new Path( fileName );
      FileSystem fs = (FileSystem) shim.getFileSystem( pentahoConf ).getDelegate();
      if ( !fs.exists( filePath ) ) {
        throw new NoSuchFileException( fileName );
      }
      if ( fs.getFileStatus( filePath ).isDirectory() ) {
        PathFilter pathFilter = file -> file.getName().endsWith( ".orc" );

        FileStatus[] fileStatuses = fs.listStatus( filePath, pathFilter );
        if ( fileStatuses.length == 0 ) {
          throw new NoSuchFileException( fileName );
        }
        filePath = fileStatuses[0].getPath();
      }
      return OrcFile.createReader( filePath,
              OrcFile.readerOptions( conf ).filesystem( fs ) );
    } catch ( IOException e ) {
      throw new IllegalArgumentException( "Unable to read data from file " + fileName, e );
    }
  }

}
