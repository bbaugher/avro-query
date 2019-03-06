package com.bb021949.query;

import com.bb021949.query.plan.QueryPlanner;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Class that can run {@link Query}s from the given avro data
 */
public class QueryRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryRunner.class);

    public static final String QUERY_LIMIT = "avro.query.limit";
    public static final int QUERY_LIMIT_DEFAULT = 100;

    public static final String QUERY_IN_MEMORY = "avro.query.in.memory";
    public static final boolean QUERY_IN_MEMORY_DEFAULT = false;

    private final Configuration conf;
    private final int queryLimit;

    /**
     * Builds the query runner
     *
     * @param conf
     *      the hadoop configuration to use
     */
    public QueryRunner(Configuration conf) {
        this.conf = conf;
        this.queryLimit = conf.getInt(QUERY_LIMIT, QUERY_LIMIT_DEFAULT);
    }

    /**
     * Runs the given query on the avro data at its specified path
     *
     * @param input
     *      the path to the avro files. If this is a directory it will search into this directories
     *      and others for avro files
     * @param query
     *      the query to calculate
     * @return the results of the query from the given data
     * @throws IOException
     *      if there is an issue running the query
     */
    public QueryResult run(Path input, Query query) throws IOException {
        Pipeline pipeline;
        if (conf.getBoolean(QUERY_IN_MEMORY, QUERY_IN_MEMORY_DEFAULT)) {
            pipeline = MemPipeline.getInstance();
            pipeline.setConfiguration(conf);
        }
        else {
            pipeline = new MRPipeline(getClass(), query + " from " + input, conf);
        }

        PCollection<GenericData.Record> records;

        if (input.getName().endsWith(".avro")) {
            records = pipeline.read(From.avroFile(input));
        }
        else {
            FileSystem fileSystem = FileSystem.get(conf);
            if (fileSystem.getFileStatus(input).isFile()) {
                records = pipeline.read(From.avroFile(input));
            }
            else {
                records = pipeline.read(From.avroFile(getAvroFiles(fileSystem, input)));
            }
        }

        QueryPlanner planner = new QueryPlanner();
        PCollection<Map<String, String>> results = planner.plan(query, records);
        Iterator<Map<String, String>> iterator = results.materialize().iterator();

        int count = 0;
        QueryResult.Builder builder = QueryResult.builder();
        List<String> headers = new ArrayList<>();
        while(iterator.hasNext()) {
            Map<String, String> row = iterator.next();

            if (headers.isEmpty()) {
                headers.addAll(row.keySet());
                builder.setHeaders(headers);
            }

            builder.addRows(headers.stream().map(key -> row.get(key)).collect(Collectors.toList()));
            count++;

            if (queryLimit != -1 && queryLimit <= count) {
                LOGGER.warn("Query limit [{}] reached", queryLimit);
                break;
            }
        }

        pipeline.done();

        return builder.build();
    }

    private List<Path> getAvroFiles(FileSystem fileSystem, Path path) throws IOException {
        List<Path> paths = new ArrayList<>();

        for (FileStatus fileStatus : fileSystem.listStatus(path)) {
            if (fileStatus.isFile() && fileStatus.getPath().getName().endsWith(".avro")) {
                LOGGER.debug("Found avro file [{}]", fileStatus.getPath());
                paths.add(fileStatus.getPath());
            }
            else if (fileStatus.isDirectory()) {
                LOGGER.debug("Searching for avro files in directory [{}]", fileStatus.getPath());
                paths.addAll(getAvroFiles(fileSystem, fileStatus.getPath()));
            }
        }

        return paths;
    }
}
