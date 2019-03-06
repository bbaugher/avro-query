package com.bb021949.query;

import com.bb021949.query.selector.Selector;
import com.bb021949.query.operator.BooleanOperator;
import org.apache.crunch.Pair;
import org.immutables.value.Value;

import java.io.Serializable;
import java.util.List;

/**
 * A search command that specifies how to filter the data
 */
@Value.Immutable
public abstract class AbstractSearchCommand implements Serializable{

    private static final long serialVersionUID = -1405722887564196828L;

    /**
     * The first selector in the search command
     *
     * @return the first selector in the search command
     */
    public abstract Selector getFirstSelector();

    /**
     * Additional pairs of {@link BooleanOperator} and {@link Selector}s for the search query
     *
     * @return the additional pairs of {@link BooleanOperator} and {@link Selector}s
     */
    public abstract List<Pair<BooleanOperator, Selector>> getSelectors();
}
