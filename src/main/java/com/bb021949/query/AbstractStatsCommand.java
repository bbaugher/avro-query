package com.bb021949.query;

import org.immutables.value.Value;

import java.util.List;
import java.util.Optional;

/**
 * A statistical command to calculate some result from the query
 */
@Value.Immutable
public abstract class AbstractStatsCommand {

    /**
     * The {@link StatsFunction} to calculate
     *
     * @return the {@link StatsFunction} to calculate
     */
    public abstract StatsFunction getStatsFunction();

    /**
     * The optional name of the field to use for the {@link StatsFunction}
     *
     * @return the optional name of the field to use for the {@link StatsFunction}
     */
    public abstract Optional<String> getFunctionField();

    /**
     * The list of fields to group the results by. Can be empty.
     *
     * @return the list of fields to group the results by. Can be empty.
     */
    public abstract List<String> getGroupFields();
}
