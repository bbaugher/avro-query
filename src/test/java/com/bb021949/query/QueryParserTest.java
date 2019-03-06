package com.bb021949.query;

import com.bb021949.query.operator.BooleanOperator;
import com.bb021949.query.operator.ValueOperator;
import com.bb021949.query.selector.AllSelector;
import com.bb021949.query.selector.FieldSelector;
import com.bb021949.query.selector.InverseSelector;
import org.apache.crunch.Pair;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class QueryParserTest {

    private QueryParser queryParser;

    @Before
    public void before() {
        queryParser = new QueryParser();
    }

    @Test
    public void noQuery() {
        Query query = queryParser.parse("");
        assertThat(query, is(
                Query.builder()
                        .setSearchCommand(
                                SearchCommand.builder()
                                        .setFirstSelector(new AllSelector())
                                        .build()
                        )
                .build()
        ));
    }

    @Test
    public void basicSearchQuery() {
        Query query = queryParser.parse("field1 > 1 field2 <= 2 OR NOT field3 = 3");
        assertThat(query, is(
            Query.builder()
                .setSearchCommand(SearchCommand.builder()
                    .setFirstSelector(FieldSelector.builder().setField("field1").setValueOperator(
                            ValueOperator.GREATER_THAN).setValue("1").build())
                    .addSelectors(Pair.of(BooleanOperator.AND, FieldSelector.builder()
                            .setField("field2")
                            .setValueOperator(ValueOperator.LESS_THAN_OR_EQUAL_TO)
                            .setValue("2")
                            .build()))
                    .addSelectors(Pair.of(BooleanOperator.OR, new InverseSelector(FieldSelector.builder()
                            .setField("field3")
                            .setValueOperator(ValueOperator.EQUALS)
                            .setValue("3")
                            .build())))
                    .build()
                )
                .build()
        ));
    }@Test
    public void onlyStats() {
        Query query = queryParser.parse(" | stats count");
        assertThat(query, is(
                Query.builder()
                        .setSearchCommand(SearchCommand.builder()
                                .setFirstSelector(new AllSelector())
                                .build()
                        )
                        .setStatsCommand(StatsCommand.builder()
                                .setStatsFunction(StatsFunction.COUNT)
                                .build())
                .build()
        ));
    }

    @Test
    public void statsFunction() {
        Query query = queryParser.parse("field1 > 1 | stats count");
        assertThat(query, is(
            Query.builder()
                .setSearchCommand(SearchCommand.builder()
                    .setFirstSelector(FieldSelector.builder().setField("field1").setValueOperator(
                            ValueOperator.GREATER_THAN).setValue("1").build())
                    .build()
                )
                .setStatsCommand(StatsCommand.builder()
                        .setStatsFunction(StatsFunction.COUNT)
                        .build())
                .build()
        ));
    }

    @Test
    public void statsFunctionWithFieldAndGroup() {
        Query query = queryParser.parse("field1 > 1 | stats sum(field1) by field2, field3");
        assertThat(query, is(
                Query.builder()
                        .setSearchCommand(SearchCommand.builder()
                                .setFirstSelector(FieldSelector.builder().setField("field1").setValueOperator(
                                        ValueOperator.GREATER_THAN).setValue("1").build())
                                .build()
                        )
                        .setStatsCommand(StatsCommand.builder()
                                .setStatsFunction(StatsFunction.SUM)
                                .setFunctionField("field1")
                                .addGroupFields("field2", "field3")
                                .build())
                .build()
        ));
    }
}
