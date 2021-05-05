package org.pentaho.hadoop.shim.hdi.format;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.format.IPentahoAvroInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoAvroOutputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcOutputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetOutputFormat;
import org.pentaho.hadoop.shim.common.CommonFormatShim;
import org.pentaho.hadoop.shim.common.format.avro.PentahoAvroInputFormat;
import org.pentaho.hadoop.shim.common.format.avro.PentahoAvroOutputFormat;
import org.pentaho.hadoop.shim.common.format.orc.PentahoOrcInputFormat;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.DelegateFormatFactory;
import org.pentaho.hadoop.shim.hdi.format.orc.HDIOrcInputFormat;
import org.pentaho.hadoop.shim.hdi.format.orc.HDIOrcOutputFormat;
import org.pentaho.hadoop.shim.hdi.format.parquet.HDIApacheInputFormat;
import org.pentaho.hadoop.shim.hdi.format.parquet.HDIApacheOutputFormat;

public class HDIFormatShim extends CommonFormatShim {

    @Override
    public <T extends IPentahoInputFormat> T  createInputFormat(Class<T> type, NamedCluster namedCluster ) {
        if ( type.isAssignableFrom( IPentahoParquetInputFormat.class ) ) {
            return (T) new HDIApacheInputFormat( namedCluster );
        } else if ( type.isAssignableFrom( IPentahoAvroInputFormat.class ) ) {
            return (T) new PentahoAvroInputFormat( namedCluster );
        } else if ( type.isAssignableFrom( IPentahoOrcInputFormat.class ) ) {
            return (T) new HDIOrcInputFormat( namedCluster );
        }
        throw new IllegalArgumentException( "Not supported scheme format" );
    }

    @Override
    public <T extends IPentahoOutputFormat> T createOutputFormat(Class<T> type, NamedCluster namedCluster ) {
        if ( type.isAssignableFrom( IPentahoParquetOutputFormat.class ) ) {
            return (T) new HDIApacheOutputFormat( namedCluster );
        } else if ( type.isAssignableFrom( IPentahoAvroOutputFormat.class ) ) {
            return (T) new PentahoAvroOutputFormat();
        } else if ( type.isAssignableFrom( IPentahoOrcOutputFormat.class ) ) {
            return (T) new HDIOrcOutputFormat( namedCluster );
        }
        throw new IllegalArgumentException( "Not supported scheme format" );
    }
}
