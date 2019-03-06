package com.bb021949.query.selector;

import com.bb021949.query.operator.ValueOperator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.immutables.value.Value;

/**
 * A {@link Selector} for filtering based on a specific field, operator and value
 */
@Value.Immutable
public abstract class AbstractFieldSelector implements Selector {

    private static final long serialVersionUID = -1693298329707639480L;

    /**
     * The {@link ValueOperator} to use to filter the data
     *
     * @return {@link ValueOperator} to use to filter the data
     */
    public abstract ValueOperator getValueOperator();

    /**
     * The name of the field to filter on
     *
     * @return the name of the field to filter on
     */
    public abstract String getField();

    /**
     * The value of the field to filter on
     *
     * @return the value of the field to filter on
     */
    public abstract String getValue();

    @Override
    public boolean select(GenericData.Record record) {

        Schema.Field field = record.getSchema().getField(getField());

        // There is no matching field so filter it out
        if (field == null) {
            return true;
        }

        Object recordValue = record.get(getField());

        // There is no value for this field
        if (recordValue == null) {
            return true;
        }

        Schema.Type type = field.schema().getType();

        // Currently only support basic types
        if (type.equals(Schema.Type.STRING) || type.equals(Schema.Type.ENUM) || type.equals(Schema.Type.BOOLEAN)) {
            // Should be able to convert boolean to string and compare. This doesn't make sense for anything but equals but works
            return compareStringValue(recordValue);
        }
        else if (type.equals(Schema.Type.INT) || type.equals(Schema.Type.LONG) || type.equals(Schema.Type.FLOAT) ||
                type.equals(Schema.Type.DOUBLE)) {
            return compareNumberValue(recordValue);
        }

        // TODO types
        // Array - contains?
        // Map - Extrapolate key/values into field . key = value and follow same pattern as above
        // Record - Same as map?
        // Bytes - gross
        // Fixed - ???
        // Null - ???
        // Union - ???

        return true;
    }

    private boolean compareNumberValue(Object recordValue) {
        double filterNumberValue = Double.parseDouble(getValue());
        double recordNumberValue;
        if (recordValue instanceof Integer) {
            recordNumberValue = (Integer) recordValue;
        }
        else if (recordValue instanceof Long) {
            recordNumberValue = (Long) recordValue;
        }
        else if (recordValue instanceof Double) {
            recordNumberValue = (Double) recordValue;
        }
        else if (recordValue instanceof Float) {
            recordNumberValue = (Float) recordValue;
        }
        else {
            throw new IllegalStateException("Unknown numeric class " + recordValue + " with class "
                    + recordValue.getClass());
        }

        if (getValueOperator().equals(ValueOperator.EQUALS))
            return filterNumberValue == recordNumberValue;

        if (getValueOperator().equals(ValueOperator.GREATER_THAN))
            return filterNumberValue < recordNumberValue;

        // Selector if they are less than
        if (getValueOperator().equals(ValueOperator.GREATER_THAN_OR_EQUAL_TO))
            return filterNumberValue <= recordNumberValue;

        // Selector if they are less than
        if (getValueOperator().equals(ValueOperator.LESS_THAN))
            return filterNumberValue > recordNumberValue;

        // Selector if they are less than
        if (getValueOperator().equals(ValueOperator.LESS_THAN_OR_EQUAL_TO))
            return filterNumberValue >= recordNumberValue;

        // Missed an operator if we get here
        return true;
    }

    private boolean compareStringValue(Object recordValue) {
        if (getValueOperator().equals(ValueOperator.EQUALS))
            return getValue().equals(recordValue.toString());

        int compare = recordValue.toString().compareToIgnoreCase(getValue());

        // -1 means recordValue is less than filterValue
        // +1 means recordValue is greater than filterValue

        if (getValueOperator().equals(ValueOperator.GREATER_THAN))
            return compare > 0;
        if (getValueOperator().equals(ValueOperator.GREATER_THAN_OR_EQUAL_TO))
            return compare >= 0;
        if (getValueOperator().equals(ValueOperator.LESS_THAN))
            return compare < 0;
        if (getValueOperator().equals(ValueOperator.LESS_THAN_OR_EQUAL_TO))
            return compare <= 0;
        if (getValueOperator().equals(ValueOperator.EQUALS))
            return compare == 0;

        // Missed an operator if we get here
        return true;
    }
}
