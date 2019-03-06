package com.bb021949.query.plan;

import com.bb021949.query.StatsFunction;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * {@link MapFn} that takes the results of grouped avro data and calculates the provided statistical
 * function.
 */
public class GroupedRecordsToStatsFn extends
        MapFn<Pair<List<Pair<String, String>>, Iterable<GenericData.Record>>, Map<String, String>> {

    private static final long serialVersionUID = -768834714851487087L;
    private final StatsFunction statsFunction;
    private final String functionField;

    /**
     * Creates the function
     *
     * @param statsFunction
     *      the stats function to calculate
     * @param functionField
     *      the stats function field
     * @throws IllegalArgumentException
     *      if {@link StatsFunction} is not {@link StatsFunction#COUNT} and functionField is empty
     *      and not provided
     */
    public GroupedRecordsToStatsFn(StatsFunction statsFunction, Optional<String> functionField) {
        this.statsFunction = statsFunction;
        this.functionField = functionField.orElse(null);

        if (!statsFunction.equals(StatsFunction.COUNT) && functionField == null)
            throw new IllegalArgumentException("Expected function field for stats function "
                    + statsFunction);
    }

    @Override
    public Map<String, String> map(Pair<List<Pair<String, String>>, Iterable<GenericData.Record>>
            pairRecords) {
        Map<String, String> row = new HashMap<>();

        for (Pair<String, String> pair : pairRecords.first()) {
            row.put(pair.first(), pair.second());
        }

        if (statsFunction.equals(StatsFunction.COUNT)) {
            long count = 0L;
            Iterator<GenericData.Record> iterator = pairRecords.second().iterator();
            while(iterator.hasNext()) {
                iterator.next();
                count++;
            }
            row.put("Count", Long.toString(count));
            return row;
        }

        if (statsFunction.equals(StatsFunction.SUM)) {
            double sum = 0.0;
            Iterator<GenericData.Record> iterator = pairRecords.second().iterator();
            while(iterator.hasNext()) {
                GenericData.Record record = iterator.next();

                if (record.get(functionField) != null)
                    sum += getNumericFieldValue(functionField, record);
            }
            row.put("Sum(" + functionField + ")", Double.toString(sum));
        }
        else if (statsFunction.equals(StatsFunction.MAX)) {
            double max = 0.0;
            Iterator<GenericData.Record> iterator = pairRecords.second().iterator();
            while(iterator.hasNext()) {
                GenericData.Record record = iterator.next();

                if (record.get(functionField) != null) {
                    double value = getNumericFieldValue(functionField, record);
                    if (max < value)
                        max = value;
                }
            }
            row.put("Max(" + functionField + ")", Double.toString(max));
        }
        else if (statsFunction.equals(StatsFunction.MIN)) {
            double min = 0.0;
            Iterator<GenericData.Record> iterator = pairRecords.second().iterator();
            while(iterator.hasNext()) {
                GenericData.Record record = iterator.next();
                if (record.get(functionField) != null) {
                    double value = getNumericFieldValue(functionField, record);
                    if (min > value)
                        min = value;
                }
            }
            row.put("Min(" + functionField + ")", Double.toString(min));
        }
        else {
            throw new IllegalStateException("Unsupported stats function " + statsFunction);
        }

        return row;
    }

    private double getNumericFieldValue(String field, GenericData.Record record) {
        Schema.Field avroField = record.getSchema().getField(field);
        if (avroField == null)
            throw new IllegalStateException("no field named " + field);

        Schema.Type fieldType = avroField.schema().getType();
        if (fieldType.equals(Schema.Type.INT) || fieldType.equals(Schema.Type.LONG) ||
                fieldType.equals(Schema.Type.FLOAT) || fieldType.equals(Schema.Type.DOUBLE)) {
            Object recordValue = record.get(field);

            if (recordValue instanceof Integer) {
                return (Integer) recordValue;
            }
            else if (recordValue instanceof Long) {
                return (Long) recordValue;
            }
            else if (recordValue instanceof Double) {
                return (Double) recordValue;
            }
            else if (recordValue instanceof Float) {
                return (Float) recordValue;
            }
        }

        throw new IllegalStateException("Unable to apply stats function to non-numeric field, " +
                avroField);
    }
}
