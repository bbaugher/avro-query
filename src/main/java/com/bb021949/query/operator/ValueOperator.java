package com.bb021949.query.operator;

import com.bb021949.query.antlr.TextQueryParser;

/**
 * The value operators supported in a search query.
 */
public enum ValueOperator {
    EQUALS(TextQueryParser.EQUALS),
    GREATER_THAN(TextQueryParser.GREATER_THAN),
    GREATER_THAN_OR_EQUAL_TO(TextQueryParser.GREATER_THAN_OR_EQUAL_TO),
    LESS_THAN(TextQueryParser.LESS_THAN),
    LESS_THAN_OR_EQUAL_TO(TextQueryParser.LESS_THAN_OR_EQUAL_TO);

    private final int type;

    /**
     * Returns the value operator associated to the type value from {@link TextQueryParser}
     *
     * @param type
     *      the type value from {@link TextQueryParser}
     * @return the matching value operator or {@link IllegalArgumentException} if none is found
     */
    public static ValueOperator getValueOperator(int type) {
        for (ValueOperator valueOperator : ValueOperator.values()) {
            if (valueOperator.type == type)
                return valueOperator;
        }

        throw new IllegalArgumentException("No operation for type " + type);
    }

    private ValueOperator(int type) {
        this.type = type;
    }
}