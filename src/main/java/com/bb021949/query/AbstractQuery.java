package com.bb021949.query;

import org.immutables.value.Value;

import java.util.Optional;

/**
 * A model that represents a query
 */
@Value.Immutable
public abstract class AbstractQuery {
    /**
     * The search command to filter the data
     *
     * @return the search command to filter the data
     */
    public abstract SearchCommand getSearchCommand();

    /**
     * An optional {@link StatsCommand} that can calculate some statistical functions
     *
     * @return an optional {@link StatsCommand}
     */
    public abstract Optional<StatsCommand> getStatsCommand();
}
