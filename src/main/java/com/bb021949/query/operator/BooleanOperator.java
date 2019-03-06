package com.bb021949.query.operator;

import com.bb021949.query.antlr.TextQueryParser;

/**
 * An enum for representing the different boolean operators supported in the query.
 */
public enum BooleanOperator {
    AND(TextQueryParser.AND), OR(TextQueryParser.OR), NOT(TextQueryParser.NOT);

    private final int type;

    /**
     * Retrieves the {@link BooleanOperator} given the {@link TextQueryParser} type value
     *
     * @param type
     *      the {@link TextQueryParser} type value
     * @return the matching {@link BooleanOperator} or {@link IllegalArgumentException} is thrown
     */
    public static BooleanOperator getBooleanOperator(int type) {
        for (BooleanOperator booleanOperator : BooleanOperator.values()) {
            if (booleanOperator.type == type)
                return booleanOperator;
        }

        throw new IllegalArgumentException("No boolean operator for type " + type);
    }

    private BooleanOperator(int type) {
        this.type = type;
    }
}
