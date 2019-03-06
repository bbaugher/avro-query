package com.bb021949.query.cli;

import io.airlift.airline.Cli;
import io.airlift.airline.Context;
import io.airlift.airline.Help;
import io.airlift.airline.ParseArgumentsMissingException;
import io.airlift.airline.ParseArgumentsUnexpectedException;
import io.airlift.airline.ParseOptionMissingException;
import io.airlift.airline.ParseOptionMissingValueException;
import io.airlift.airline.ParseState;
import io.airlift.airline.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Callable;

/**
 * The main class for the CLI
 */
public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    /**
     * The main method
     *
     * @param args
     *      the arguments provided to the CLI
     * @throws IOException
     *      if there is an issue running the CLI
     */
    public static void main(String[] args) throws IOException {
        System.exit(new Main().run(args));
    }

    protected Cli<Callable<Void>> getCli() {
        // We use Callable here instead of Runnable since Callable allows us to throw exceptions
        Cli.CliBuilder<Callable<Void>> builder = Cli.<Callable<Void>>builder("avro-query")
                .withDescription("CLI for querying about avro data on HDFS")
                .withDefaultCommand(Help.class)
                .withCommands(QueryCommand.class, Help.class);

        return builder.build();
    }

    /**
     * Runs the CLI with the given arguments
     *
     * @param args
     *      the arguments for the CLI
     * @return the exit code
     */
    public int run(String[] args) {
        if (LOG.isDebugEnabled())
            LOG.debug("Running command with args {}", Arrays.asList(args));

        Cli<Callable<Void>> cli = getCli();

        try {
            cli.parse(args).call();
        } catch (ParseArgumentsMissingException | ParseOptionMissingValueException | ParseOptionMissingException e) {
            // These exceptions mean it found the command but was missing arguments/options/values
            LOG.debug("Parse exception", e);
            System.out.println(e.getMessage());
            printHelp(cli, args);

            return 1;
        } catch (ParseArgumentsUnexpectedException e) {
            // This exception means the parser could not find a valid command for the arguments
            LOG.debug("Parse exception", e);
            System.out.println("Command not found or unexpected options passed");
            printHelp(cli, args);

            return 1;
        } catch (Exception e) {
            LOG.debug("Command exception", e);

            // For any other exceptions print the whole stack trace
            e.printStackTrace(System.out);
            return 1;
        }

        return 0;
    }

    protected void printHelp(Cli cli, String[] args) {
        ParseState state = new Parser().parse(cli.getMetadata(), args);

        if (state.getLocation().equals(Context.GLOBAL)) {
            Help.help(cli.getMetadata(), Collections.emptyList());
        } else if (state.getLocation().equals(Context.GROUP)) {
            Help.help(cli.getMetadata(), Collections.singletonList(state.getGroup().getName()));
        } else if (state.getLocation().equals(Context.COMMAND)) {
            Help.help(state.getCommand());
        } else {
            LOG.error("Parse state has unsupported location: {}", state.getLocation());
        }
    }
}
