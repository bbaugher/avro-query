package com.bb021949.query.cli;

import com.bb021949.query.Query;
import com.bb021949.query.QueryResult;
import com.bb021949.query.QueryRunner;
import com.bb021949.query.QueryParser;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.crunch.util.DistCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * The query command that parses a string query and runs the query over the avro data
 */
@Command(name = "query", description = "Runs a query over the avro data")
public class QueryCommand implements Callable<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryCommand.class);

    @Arguments(description = "The first argument is the path to the avro files, everything else "
            + "afterwards is the query")
    private List<String> arguments;

    @Option(name = {"-i", "--in-memory"}, description =
            "if the query should run in-memory instead of using map/reduce")
    private boolean inMemory = false;

    @Option(name = {"-l", "--limit"}, description = "the results limit for the query. Defaults to "
            + QueryRunner.QUERY_IN_MEMORY_DEFAULT)
    private int queryLimit = QueryRunner.QUERY_LIMIT_DEFAULT;

    @Override
    public Void call() throws Exception {
        QueryParser parser = new QueryParser();

        String pathValue = arguments.remove(0);

        Query query = parser.parse(String.join(" ", arguments));
        LOGGER.info("Parsed query {}", query);

        Path path = new Path(pathValue);
        LOGGER.info("Reading avro data at {}", path);

        String libDir = System.getProperty("avro.query.lib.dir");

        Configuration conf = new Configuration();
        conf.set(QueryRunner.QUERY_IN_MEMORY, Boolean.toString(inMemory));
        conf.set(QueryRunner.QUERY_LIMIT, Integer.toString(queryLimit));

        // We aren't using a shaded jar so we have to tell hadoop about our lib directory to ensure
        // our classes are available to map/reduce
        DistCache.addJarDirToDistributedCache(conf, new File(libDir));

        QueryRunner runner = new QueryRunner(conf);

        LOGGER.info("Running query ...");
        QueryResult result = runner.run(path, query);

        System.out.println(String.join(", ", result.getHeaders()));
        for (List<String> row : result.getRows()) {
            System.out.println(String.join(", ", row));
        }

        return null;
    }
}
