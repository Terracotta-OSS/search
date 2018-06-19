/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldValueFilter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;

import com.terracottatech.search.AbstractNVPair.BooleanNVPair;
import com.terracottatech.search.AbstractNVPair.ByteNVPair;
import com.terracottatech.search.AbstractNVPair.CharNVPair;
import com.terracottatech.search.AbstractNVPair.DateNVPair;
import com.terracottatech.search.AbstractNVPair.DoubleNVPair;
import com.terracottatech.search.AbstractNVPair.EnumNVPair;
import com.terracottatech.search.AbstractNVPair.FloatNVPair;
import com.terracottatech.search.AbstractNVPair.IntNVPair;
import com.terracottatech.search.AbstractNVPair.LongNVPair;
import com.terracottatech.search.AbstractNVPair.ShortNVPair;
import com.terracottatech.search.AbstractNVPair.SqlDateNVPair;
import com.terracottatech.search.AbstractNVPair.StringNVPair;
import com.terracottatech.search.LuceneIndexManager.AttributeProperties;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

public class LuceneQueryBuilder {

  private final List                             queryStack;
  private final Map<String, AttributeProperties> indexSchema;

  public LuceneQueryBuilder(List queryStack, Map<String, AttributeProperties> indexSchema) {
    this.queryStack = queryStack;
    this.indexSchema = indexSchema;
  }

  public Query buildQuery() throws IndexException {
    LinkedList<Group> stack = new LinkedList<Group>();

    for (Iterator iter = queryStack.iterator(); iter.hasNext();) {
      StackOperations op = (StackOperations) iter.next();
      if (op == StackOperations.BEGIN_GROUP) {
        op = (StackOperations) iter.next();
        stack.push(new Group(op));
      } else if (op == StackOperations.END_GROUP) {
        Query query = stack.pop().query();
        if (stack.isEmpty()) return query;
        stack.peek().add(query);
      } else {
        Query query = processQuery(op, iter);
        if (stack.isEmpty()) return query;
        stack.peek().add(query);
      }
    }

    if (!queryStack.isEmpty()) throw new IndexException("Unexpected EOF in query stack: " + queryStack);
    else return new MatchAllDocsQuery();
  }

  private static Occur occurFor(StackOperations op) {
    if (op == StackOperations.AND) { return Occur.MUST; }
    if (op == StackOperations.OR) { return Occur.SHOULD; }
    throw new AssertionError("operation: " + op);
  }

  private Query processQuery(StackOperations operation, Iterator iter) throws IndexException {

    switch (operation) {
      case BETWEEN:
        return processBetween(iter);
      case GREATER_THAN:
        return processGreaterThan(iter);
      case GREATER_THAN_EQUAL:
        return processGreaterThanEqual(iter);
      case LESS_THAN:
        return processLessThan(iter);
      case LESS_THAN_EQUAL:
        return processLessThanEqual(iter);
      case ILIKE:
        return processILike(iter);
      case TERM:
        return processTerm(iter);
      case NOT_TERM:
        return processNotTerm(iter);
      case NULL:
        return processNull(iter, true);
      case NOT_NULL:
        return processNull(iter, false);
      case ALL:
        return new MatchAllDocsQuery();
      case NOT_ILIKE:
        return processNotILike(iter);
      case AND:
      case OR:
      case BEGIN_GROUP:
      case END_GROUP:
        throw new IndexException(operation.name() + " is not expected here");
    }

    throw new AssertionError("operation: " + operation);
  }

  private Query processBetween(Iterator iter) throws IndexException {
    NVPair minRange = (NVPair) iter.next();
    NVPair maxRange = (NVPair) iter.next();
    NVPair minInclusive = (NVPair) iter.next();
    NVPair maxInclusive = (NVPair) iter.next();

    verifyType(minRange);

    String name = minRange.getName();
    boolean minInclude = ((BooleanNVPair) minInclusive).getValue();
    boolean maxInclude = ((BooleanNVPair) maxInclusive).getValue();

    ValueType type = minRange.getType();
    switch (type) {
      case BOOLEAN: {
        Integer min = ((BooleanNVPair) minRange).getValue() ? 1 : 0;
        Integer max = ((BooleanNVPair) maxRange).getValue() ? 1 : 0;
        return NumericRangeQuery.newIntRange(name, min, max, minInclude, maxInclude);
      }
      case BYTE_ARRAY:
        throw new IndexException(type.name() + " not supported");
      case BYTE: {
        Integer min = Integer.valueOf(((ByteNVPair) minRange).getValue());
        Integer max = Integer.valueOf(((ByteNVPair) maxRange).getValue());
        return NumericRangeQuery.newIntRange(name, min, max, minInclude, maxInclude);
      }
      case CHAR: {
        Integer min = (int) ((CharNVPair) minRange).getValue();
        Integer max = (int) ((CharNVPair) maxRange).getValue();
        return NumericRangeQuery.newIntRange(name, min, max, minInclude, maxInclude);
      }
      case DOUBLE: {
        Double min = ((DoubleNVPair) minRange).getValue();
        Double max = ((DoubleNVPair) maxRange).getValue();
        return NumericRangeQuery.newDoubleRange(name, min, max, minInclude, maxInclude);
      }
      case FLOAT: {
        Float min = ((FloatNVPair) minRange).getValue();
        Float max = ((FloatNVPair) maxRange).getValue();
        return NumericRangeQuery.newFloatRange(name, min, max, minInclude, maxInclude);
      }
      case INT: {
        Integer min = ((IntNVPair) minRange).getValue();
        Integer max = ((IntNVPair) maxRange).getValue();
        return NumericRangeQuery.newIntRange(name, min, max, minInclude, maxInclude);
      }
      case LONG: {
        Long min = ((LongNVPair) minRange).getValue();
        Long max = ((LongNVPair) maxRange).getValue();
        return NumericRangeQuery.newLongRange(name, min, max, minInclude, maxInclude);
      }
      case SHORT: {
        Integer min = Integer.valueOf(((ShortNVPair) minRange).getValue());
        Integer max = Integer.valueOf(((ShortNVPair) maxRange).getValue());
        return NumericRangeQuery.newIntRange(name, min, max, minInclude, maxInclude);
      }
      case DATE: {
        Long min = ((DateNVPair) minRange).getValue().getTime();
        Long max = ((DateNVPair) maxRange).getValue().getTime();
        return NumericRangeQuery.newLongRange(name, min, max, minInclude, maxInclude);
      }
      case SQL_DATE: {
        Long min = ((SqlDateNVPair) minRange).getValue().getTime();
        Long max = ((SqlDateNVPair) maxRange).getValue().getTime();
        return NumericRangeQuery.newLongRange(name, min, max, minInclude, maxInclude);
      }
      case ENUM: {
        String min = AbstractNVPair.enumStorageString((EnumNVPair) minRange);
        String max = AbstractNVPair.enumStorageString((EnumNVPair) maxRange);
        return new TermRangeQuery(name, new BytesRef(min), new BytesRef(max), minInclude, maxInclude);
      }
      case STRING: {
        String min = ((StringNVPair) minRange).getValue();
        String max = ((StringNVPair) maxRange).getValue();
        return new TermRangeQuery(name, new BytesRef(min.toLowerCase()), new BytesRef(max.toLowerCase()), minInclude,
                                  maxInclude);
      }
      case NULL: {
        throw new IndexException(type.name() + " not supported");
      }
      case VALUE_ID:
        throw new IndexException(type.name() + " not supported");
    }

    throw new AssertionError(minRange);
  }

  private void verifyType(NVPair nvPair) throws IndexException {
    AttributeProperties attrProps = indexSchema.get(nvPair.getName());
    String typeString = LuceneIndexManager.getAttributeTypeString(nvPair);
    if (attrProps != null && !attrProps.getType().equals(typeString)) {
      //
      throw new IndexException("Expected " + attrProps.getType() + " for attribute [" + nvPair.getName()
                               + "], but was " + typeString);
    }
  }

  private Query processGreaterThan(Iterator iter) throws IndexException {
    NVPair greaterThanTerm = (NVPair) iter.next();
    return processAboveBelow(greaterThanTerm, false, false);
  }

  private Query processGreaterThanEqual(Iterator iter) throws IndexException {
    NVPair greaterThanEqualTerm = (NVPair) iter.next();
    return processAboveBelow(greaterThanEqualTerm, false, true);
  }

  private Query processLessThan(Iterator iter) throws IndexException {
    NVPair lessThanTerm = (NVPair) iter.next();
    return processAboveBelow(lessThanTerm, true, false);
  }

  private Query processLessThanEqual(Iterator iter) throws IndexException {
    NVPair lessThanEqualTerm = (NVPair) iter.next();
    return processAboveBelow(lessThanEqualTerm, true, true);
  }

  private Query processAboveBelow(NVPair term, boolean below, boolean equal) throws IndexException {
    verifyType(term);

    final boolean minInclude;
    final boolean maxInclude;

    if (below) {
      minInclude = true;
      maxInclude = equal;
    } else {
      minInclude = equal;
      maxInclude = true;
    }

    ValueType type = term.getType();
    switch (type) {
      case BOOLEAN: {
        Integer min = below ? null : (((BooleanNVPair) term).getValue() ? 1 : 0);
        Integer max = below ? (((BooleanNVPair) term).getValue() ? 1 : 0) : null;
        return NumericRangeQuery.newIntRange(term.getName(), min, max, minInclude, maxInclude);
      }
      case BYTE_ARRAY:
        throw new IndexException(type.name() + " not supported");
      case BYTE: {
        Integer min = below ? null : Integer.valueOf(((ByteNVPair) term).getValue());
        Integer max = below ? Integer.valueOf(((ByteNVPair) term).getValue()) : null;
        return NumericRangeQuery.newIntRange(term.getName(), min, max, minInclude, maxInclude);
      }
      case CHAR: {
        Integer min = below ? null : (int) ((CharNVPair) term).getValue();
        Integer max = below ? (int) ((CharNVPair) term).getValue() : null;
        return NumericRangeQuery.newIntRange(term.getName(), min, max, minInclude, maxInclude);
      }
      case DOUBLE: {
        Double min = below ? null : ((DoubleNVPair) term).getValue();
        Double max = below ? ((DoubleNVPair) term).getValue() : null;
        return NumericRangeQuery.newDoubleRange(term.getName(), min, max, minInclude, maxInclude);
      }
      case FLOAT: {
        Float min = below ? null : ((FloatNVPair) term).getValue();
        Float max = below ? ((FloatNVPair) term).getValue() : null;
        return NumericRangeQuery.newFloatRange(term.getName(), min, max, minInclude, maxInclude);
      }
      case INT: {
        Integer min = below ? null : ((IntNVPair) term).getValue();
        Integer max = below ? ((IntNVPair) term).getValue() : null;
        return NumericRangeQuery.newIntRange(term.getName(), min, max, minInclude, maxInclude);
      }
      case LONG: {
        Long min = below ? null : ((LongNVPair) term).getValue();
        Long max = below ? ((LongNVPair) term).getValue() : null;
        return NumericRangeQuery.newLongRange(term.getName(), min, max, minInclude, maxInclude);
      }
      case SHORT: {
        Integer min = below ? null : Integer.valueOf(((ShortNVPair) term).getValue());
        Integer max = below ? Integer.valueOf(((ShortNVPair) term).getValue()) : null;
        return NumericRangeQuery.newIntRange(term.getName(), min, max, minInclude, maxInclude);
      }
      case DATE: {
        Long min = below ? null : ((DateNVPair) term).getValue().getTime();
        Long max = below ? ((DateNVPair) term).getValue().getTime() : null;
        return NumericRangeQuery.newLongRange(term.getName(), min, max, minInclude, maxInclude);
      }
      case SQL_DATE: {
        Long min = below ? null : ((SqlDateNVPair) term).getValue().getTime();
        Long max = below ? ((SqlDateNVPair) term).getValue().getTime() : null;
        return NumericRangeQuery.newLongRange(term.getName(), min, max, minInclude, maxInclude);
      }
      case ENUM: {
        BytesRef min = below ? null : new BytesRef(AbstractNVPair.enumStorageString((EnumNVPair) term));
        BytesRef max = below ? new BytesRef(AbstractNVPair.enumStorageString((EnumNVPair) term)) : null;
        return new TermRangeQuery(term.getName(), min, max, minInclude, maxInclude);
      }
      case STRING: {
        BytesRef min = below ? null : new BytesRef(((StringNVPair) term).getValue().toLowerCase());
        BytesRef max = below ? new BytesRef(((StringNVPair) term).getValue().toLowerCase()) : null;
        return new TermRangeQuery(term.getName(), min, max, minInclude, maxInclude);
      }
      case NULL: {
        throw new IndexException(type.name() + " not supported");
      }
      case VALUE_ID:
        throw new IndexException(type.name() + " not supported");
    }

    throw new AssertionError(term.toString());
  }

  private Query processILike(Iterator iter) {
    NVPair likeTerm = (NVPair) iter.next();
    // XXX: is there type verification here?
    return new WildcardQuery(new Term(likeTerm.getName(), likeTerm.valueAsString().toLowerCase()));
  }

  private Query processNull(Iterator iter, boolean negate) {
    NVPair nullTerm = (NVPair) iter.next();
    return new ConstantScoreQuery(new FieldValueFilter(nullTerm.getName(), negate));
  }

  private Query processNotILike(Iterator iter) {
    NVPair likeTerm = (NVPair) iter.next();
    // XXX: is there type verification here?
    Query wcq = new WildcardQuery(new Term(likeTerm.getName(), likeTerm.valueAsString().toLowerCase()));

    BooleanQuery query = new BooleanQuery();
    query.add(new MatchAllDocsQuery(), Occur.MUST);
    query.add(wcq, Occur.MUST_NOT);
    return query;
  }

  private Query processNotTerm(Iterator iter) throws IndexException {
    Query termQuery = processTerm(iter);

    BooleanQuery query = new BooleanQuery();
    query.add(new MatchAllDocsQuery(), Occur.MUST);
    query.add(termQuery, Occur.MUST_NOT);

    return query;
  }

  private Query processTerm(Iterator iter) throws IndexException {
    NVPair term = (NVPair) iter.next();

    verifyType(term);

    ValueType type = term.getType();
    switch (type) {
      case BOOLEAN:
        BooleanNVPair booleanNVPair = (BooleanNVPair) term;
        int value = booleanNVPair.getValue() ? 1 : 0;
        return numericTermQuery(term.getName(), value);
      case BYTE:
        ByteNVPair bytePair = (ByteNVPair) term;
        return numericTermQuery(term.getName(), bytePair.getValue());
      case BYTE_ARRAY:
        throw new IndexException(type.name() + " not supported");
      case CHAR:
        CharNVPair charPair = (CharNVPair) term;
        return numericTermQuery(term.getName(), charPair.getValue());
      case DATE:
        DateNVPair datePair = (DateNVPair) term;
        long dateLong = datePair.getValue().getTime();
        return numericTermQuery(term.getName(), dateLong);
      case SQL_DATE:
        SqlDateNVPair sqlDatePair = (SqlDateNVPair) term;
        long sqlDateLong = sqlDatePair.getValue().getTime();
        return numericTermQuery(term.getName(), sqlDateLong);
      case DOUBLE:
        DoubleNVPair doublePair = (DoubleNVPair) term;
        return numericTermQuery(term.getName(), doublePair.getValue());
      case ENUM:
        EnumNVPair enumPair = (EnumNVPair) term;
        return new TermQuery(new Term(term.getName(), AbstractNVPair.enumStorageString(enumPair)));
      case FLOAT:
        FloatNVPair floatPair = (FloatNVPair) term;
        return numericTermQuery(term.getName(), floatPair.getValue());
      case INT:
        IntNVPair intPair = (IntNVPair) term;
        return numericTermQuery(term.getName(), intPair.getValue());
      case LONG:
        LongNVPair longPair = (LongNVPair) term;
        return numericTermQuery(term.getName(), longPair.getValue());
      case SHORT:
        ShortNVPair shortPair = (ShortNVPair) term;
        return numericTermQuery(term.getName(), shortPair.getValue());
      case STRING:
        break;
      case NULL:
        throw new IndexException(type.name() + " not supported");
      case VALUE_ID:
        throw new IndexException(type.name() + " not supported");
    }
    return new TermQuery(new Term(term.getName(), term.valueAsString().toLowerCase()));
  }

  private Query numericTermQuery(String name, double value) {
    return NumericRangeQuery.newDoubleRange(name, value, value, true, true);
  }

  private Query numericTermQuery(String name, float value) {
    return NumericRangeQuery.newFloatRange(name, value, value, true, true);
  }

  private Query numericTermQuery(String name, int value) {
    return NumericRangeQuery.newIntRange(name, value, value, true, true);
  }

  private Query numericTermQuery(String name, long value) {
    return NumericRangeQuery.newLongRange(name, value, value, true, true);
  }

  private static class Group {
    private final List<BooleanClause> clauses = new ArrayList<BooleanClause>();
    private final StackOperations     op;

    Group(StackOperations op) {
      this.op = op;
    }

    Query query() {
      BooleanQuery query = new BooleanQuery();
      for (BooleanClause clause : clauses) {
        query.add(clause);
      }
      return query;
    }

    void add(Query query) {
      clauses.add(new BooleanClause(query, occurFor(op)));
    }
  }

}
