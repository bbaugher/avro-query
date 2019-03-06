package com.bb021949.query;

import org.immutables.value.Value;

import java.util.List;

/**
 * The results of a query
 */
@Value.Immutable
public abstract class AbstractQueryResult {
    /**
     * The names of the headers or fields for each row in order
     *
     * @return the names of the headers or fields for each row in order
     */
    public abstract List<String> getHeaders();

    /**
     * The list of rows and their values
     *
     * @return the list of rows and their values
     */
    public abstract List<List<String>> getRows();
}
