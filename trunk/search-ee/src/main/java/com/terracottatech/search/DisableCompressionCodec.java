/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40StoredFieldsFormat;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsFormat;
import org.apache.lucene.codecs.lucene46.Lucene46Codec;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;

/**
 * Customization of Lucene41Codec that disables stored fields compression
 */
public final class DisableCompressionCodec extends FilterCodec {
  private static final String             codecName = "DisableCompressionCodec";
  private final StoredFieldsFormat sfFmt     = new Lucene40StoredFieldsFormat();
  
  private final PostingsFormat psFmt = new PerFieldPostingsFormat() {
    private final PostingsFormat fmt = new Lucene41PostingsFormat(50, 98);

    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
      return fmt;
    }
  };

  public DisableCompressionCodec() {
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
