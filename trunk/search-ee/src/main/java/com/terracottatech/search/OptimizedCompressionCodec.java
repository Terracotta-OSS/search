/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.compressing.CompressingStoredFieldsFormat;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsFormat;
import org.apache.lucene.codecs.lucene46.Lucene46Codec;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;

public final class OptimizedCompressionCodec extends FilterCodec {

  private static final String             codecName = "OptimizedCompressionCodec";
  private static final StoredFieldsFormat sfFmt     = new CompressingStoredFieldsFormat(
                                                                                        "TCStoredFieldsFmt",
                                                                                        CompressionMode.FAST_DECOMPRESSION,
                                                                                        1 << 11);
  private final PostingsFormat psFmt = new PerFieldPostingsFormat() {
    private final PostingsFormat fmt = new Lucene41PostingsFormat(50, 98);

    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
      return fmt;
    }
  };
  
  public OptimizedCompressionCodec() {
    super(codecName, new Lucene46Codec());
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return sfFmt;
  }

  @Override
  public PostingsFormat postingsFormat() {
    return psFmt;
  }
  
  

}
