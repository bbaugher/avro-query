package com.bb021949.query.selector;

import com.bb021949.query.operator.BooleanOperator;
import org.apache.avro.generic.GenericData;

import java.util.Objects;

/**
 * A {@link Selector} that combines two {@link Selector}s with a {@link BooleanOperator}
 */
public class CombinedSelector implements Selector {

    private static final long serialVersionUID = -2236867213465293012L;
    private final Selector selector1;
    private final BooleanOperator booleanOperator;
    private final Selector selector2;

    public CombinedSelector(Selector selector1, BooleanOperator booleanOperator,
            Selector selector2) {
        this.selector1 = selector1;
        this.booleanOperator = booleanOperator;
        this.selector2 = selector2;
    }

    @Override
    public boolean select(GenericData.Record record) {
        if (booleanOperator.equals(BooleanOperator.OR)) {
            return selector1.select(record) || selector2.select(record);
        }
        else if (booleanOperator.equals(BooleanOperator.AND)) {
            return selector1.select(record) && selector2.select(record);
        }
        else if (booleanOperator.equals(BooleanOperator.NOT)) {
            return selector1.select(record) && new InverseSelector(selector2).select(record);
        }

        throw new IllegalStateException("Unsupported boolean operator " + booleanOperator);
    }

    @Override
    public String toString() {
        return "CombinedSelector{" + "selector1=" + selector1 + ", booleanOperator="
                + booleanOperator + ", selector2=" + selector2 + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        CombinedSelector that = (CombinedSelector) o;
        return Objects.equals(selector1, that.selector1) && booleanOperator == that.booleanOperator
                && Objects.equals(selector2, that.selector2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(selector1, booleanOperator, selector2);
    }
}
