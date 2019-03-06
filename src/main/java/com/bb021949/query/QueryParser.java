package com.bb021949.query;

import com.bb021949.query.antlr.TextQueryLexer;
import com.bb021949.query.antlr.TextQueryParser;
import com.bb021949.query.selector.AllSelector;
import com.bb021949.query.operator.BooleanOperator;
import com.bb021949.query.selector.FieldSelector;
import com.bb021949.query.selector.InverseSelector;
import com.bb021949.query.selector.Selector;
import com.bb021949.query.operator.ValueOperator;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.crunch.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Class that can parse a String value into a {@link Query}
 */
public class QueryParser {

    /**
     * Parses the given string value into a {@link Query}
     *
     * @param query
     *      the query in text form
     * @return the text query into {@link Query}
     */
    public Query parse(String query) {

        Query.Builder queryBuilder = Query.builder();

        TextQueryLexer queryLexer = new TextQueryLexer(new ANTLRInputStream(query));
        TextQueryParser queryParser = new TextQueryParser(new CommonTokenStream(queryLexer));

        TextQueryParser.QueryContext queryContext = queryParser.query();

        Pair<Selector, List<Pair<BooleanOperator, Selector>>> searchParts = queryParts(
                queryContext.search_command().statement());

        Selector firstSelector = searchParts.first() == null ? new AllSelector() :
                searchParts.first();

        queryBuilder.setSearchCommand(
                SearchCommand.builder()
                .setFirstSelector(firstSelector)
                .setSelectors(searchParts.second())
                .build()
        );


        if (queryContext.stats_command() != null) {
            StatsCommand.Builder builder = StatsCommand.builder();

            TextQueryParser.Stats_commandContext statsContext = queryContext.stats_command();

            if (statsContext.stats_keyword().COUNT() != null) {
                builder.setStatsFunction(StatsFunction.COUNT);
            }
            else {
                if (statsContext.stats_keyword().sumFunction() != null) {
                    builder.setStatsFunction(StatsFunction.SUM);

                    String functionField = statsContext.stats_keyword().sumFunction().VALUE().getText();
                    builder.setFunctionField(functionField);
                }
                else if (statsContext.stats_keyword().maxFunction() != null) {
                    builder.setStatsFunction(StatsFunction.MAX);

                    String functionField = statsContext.stats_keyword().maxFunction().VALUE().getText();
                    builder.setFunctionField(functionField);
                }
                else if (statsContext.stats_keyword().minFunction() != null) {
                    builder.setStatsFunction(StatsFunction.MIN);

                    String functionField = statsContext.stats_keyword().minFunction().VALUE().getText();
                    builder.setFunctionField(functionField);
                }
            }

            List<String> groupByFields = new ArrayList<>();

            for(TerminalNode node : statsContext.VALUE()) {
                groupByFields.add(node.getText());
            }

            builder.setGroupFields(groupByFields);

            queryBuilder.setStatsCommand(builder.build());
        }

        return queryBuilder.build();
    }

    private Pair<Selector, List<Pair<BooleanOperator, Selector>>> queryParts(
            List<TextQueryParser.StatementContext> contexts) {

        boolean notOperator = false;
        BooleanOperator booleanOperator = null;
        List<Pair<BooleanOperator, Selector>> selectorList = new ArrayList<>();
        Pair<Selector, List<Pair<BooleanOperator, Selector>>> queryParts = Pair.of(null,
                selectorList);

        for (TextQueryParser.StatementContext statementContext : contexts) {

            TextQueryParser.KeywordContext keywordContext = statementContext.keyword();
            TextQueryParser.SelectContext selectContext = statementContext.select();

            if (keywordContext != null) {
                int booleanOperationType = keywordContext.getStart().getType();
                BooleanOperator nextBooleanOperator = BooleanOperator.getBooleanOperator(booleanOperationType);

                if (nextBooleanOperator.equals(BooleanOperator.NOT)) {
                    notOperator = true;
                }
                else {
                    booleanOperator = nextBooleanOperator;
                }
            } else if (selectContext != null) {
                String fieldName = selectContext.VALUE(0).getText();

                int valueOperationType = selectContext.valueOperator().getStart().getType();
                ValueOperator valueOperator = ValueOperator.getValueOperator(valueOperationType);

                String value = selectContext.VALUE(1).getText();

                Selector selector = FieldSelector.builder()
                        .setField(fieldName)
                        .setValueOperator(valueOperator)
                        .setValue(value)
                        .build();

                if (queryParts.first() == null) {
                    queryParts = Pair.of(selector, selectorList);
                }
                else {
                    if (notOperator) {
                        selector = new InverseSelector(selector);
                    }

                    if (booleanOperator == null)
                        booleanOperator = BooleanOperator.AND;

                    selectorList.add(Pair.of(booleanOperator, selector));

                    booleanOperator = null;
                    notOperator = false;
                }
            }

        }

        return queryParts;
    }
}
