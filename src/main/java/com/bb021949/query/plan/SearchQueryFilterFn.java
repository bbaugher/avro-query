package com.bb021949.query.plan;

import com.bb021949.query.SearchCommand;
import com.bb021949.query.operator.BooleanOperator;
import com.bb021949.query.selector.CombinedSelector;
import com.bb021949.query.selector.Selector;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.FilterFn;
import org.apache.crunch.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

/**
 * Function that filters {@link GenericData.Record} by the given {@link SearchCommand}
 */
public class SearchQueryFilterFn extends FilterFn<GenericData.Record> implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SearchQueryFilterFn.class);

    private static final long serialVersionUID = -5292690213662470316L;
    private final SearchCommand searchQuery;
    private Selector selector;

    /**
     * Creates the filter function
     *
     * @param searchQuery
     *      the search query to use to filter the data
     */
    public SearchQueryFilterFn(SearchCommand searchQuery) {
        this.searchQuery = searchQuery;
    }

    @Override
    public void initialize() {
        selector = combineSelectors(searchQuery.getFirstSelector(), searchQuery.getSelectors());
        LOGGER.info("Using combined selector {}", selector);
    }

    @Override
    public boolean accept(GenericData.Record record) {
        return selector.select(record);
    }

    private Selector combineSelectors(Selector selector, List<Pair<BooleanOperator, Selector>>
            selectors) {
        if (selectors.isEmpty()) {
            return selector;
        }

        Pair<BooleanOperator, Selector> next = selectors.get(0);
        List<Pair<BooleanOperator, Selector>> remaining = selectors.subList(1, selectors.size());

        return new CombinedSelector(selector, next.first(),
                combineSelectors(next.second(), remaining));
    }


}
