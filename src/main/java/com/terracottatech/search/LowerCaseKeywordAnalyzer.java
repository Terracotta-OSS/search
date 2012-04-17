/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import java.io.IOException;
import java.io.Reader;

public final class LowerCaseKeywordAnalyzer extends KeywordAnalyzer {

  @Override
  public LowerCaseFilter tokenStream(String fieldName, Reader reader) {
    return new LowerCaseFilter(super.tokenStream(fieldName, reader));
  }

  @Override
  public TokenStream reusableTokenStream(String fieldName, final Reader reader) throws IOException {
    TokenStream stream = super.reusableTokenStream(fieldName, reader);
    if (!(stream instanceof LowerCaseFilter)) {
      stream = new LowerCaseFilter(stream);
    }
    return stream;
  }

  /**
   * This filter uses java.lang.String lowercase logic which can be slightly different
   * than the LowerCaseFilter in lucene-core. In particular the handling of greek sigma differs<br>
   * <br>
   * http://bugs.sun.com/view_bug.do?bug_id=4217441
   */
  private static class LowerCaseFilter extends TokenFilter {
    private final TermAttribute termAtt = addAttribute(TermAttribute.class);

    public LowerCaseFilter(TokenStream in) {
      super(in);
    }

    @Override
    public final boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        char[] buffer = termAtt.termBuffer();
        int origLength = termAtt.termLength();

        String lower = new String(buffer, 0, origLength).toLowerCase();
        int newLength = lower.length();
        if (newLength != origLength) {
          termAtt.resizeTermBuffer(newLength);
          termAtt.setTermLength(newLength);
        }

        for (int i = 0; i < newLength; i++) {
          buffer[i] = lower.charAt(i);
        }
        return true;
      } else {
        return false;
      }
    }
  }

}
