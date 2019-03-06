package com.bb021949.query.plan;

import com.bb021949.query.Query;
import com.bb021949.query.StatsCommand;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Plans the given {@link Query} over the {@link PCollection<GenericData.Record>} to
 * provide the results of the query in {@link PCollection<Map<String, String>>}.
 */
public class QueryPlanner {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryPlanner.class);

    // Convert query and records into some result

    /**
     * Plans the appropriate functions to generate the results of the query from the given data
     *
     * @param query
     *      the query to run
     * @param records
     *      the records to run the query on
     * @return the results of the query
     */
    public PCollection<Map<String, String>> plan(Query query,
            PCollection<GenericData.Record> records) {

        PCollection<GenericData.Record> searchedRecords = records.filter(
                new SearchQueryFilterFn(query.getSearchCommand()));

        if (!query.getStatsCommand().isPresent()) {
            LOGGER.debug("No stats command present, running select and returning results");
            return searchedRecords.parallelDo(new RecordToRowListMapFn(),
                    Avros.maps(Avros.strings()));
        }

        StatsCommand statsCommand = query.getStatsCommand().get();

        // TODO - If there are no grouping fields this will force to a single reducer, we should
        // be able to do a combiner as well

        LOGGER.debug("Stats command present, grouping data and calculating stats function");

        return searchedRecords.parallelDo(new GenericRecordTableMapFn(
                statsCommand.getGroupFields()), Avros.tableOf(LIST_PTYPE,
                searchedRecords.getPType()))
                .groupByKey()
                .parallelDo(new GroupedRecordsToStatsFn(statsCommand.getStatsFunction(),
                        statsCommand.getFunctionField()), Avros.maps(Avros.strings()));
    }

    // Avro maps are not comparable to be used as key in map/reduce so use this workaround instead
    private static final PType<List<Pair<String, String>>> LIST_PTYPE =
            Avros.derived((Class<List<Pair<String, String>>>)(Object) List.class,
                    new StringToListFn(), new ListToStringFn(), Avros.strings());

    private static class ListToStringFn extends MapFn<List<Pair<String, String>>, String> {
        private static final long serialVersionUID = -3943889440170267607L;
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
        @Override
        public String map(List<Pair<String, String>> pairs) {
            Map<String, String> map = new HashMap<>();
            for (Pair<String, String> pair : pairs) {
                map.put(pair.first(), pair.second());
            }

            try {
                return OBJECT_MAPPER.writeValueAsString(map);
            } catch (JsonProcessingException e) {
                throw new CrunchRuntimeException("error converting list to string", e);
            }
        }
    }

    private static class StringToListFn extends MapFn<String, List<Pair<String, String>>> {
        private static final long serialVersionUID = 5272792831851726471L;
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
        @Override
        public List<Pair<String, String>> map(String string) {
            Map<String, String> map = null;
            try {
                map = OBJECT_MAPPER.readValue(string, Map.class);
            } catch (IOException e) {
                throw new CrunchRuntimeException("error converting string to list", e);
            }

            List<Pair<String, String>> list = new ArrayList<>();
            map.forEach((k, v) -> list.add(Pair.of(k, v)));
            return list;
        }
    }

}
