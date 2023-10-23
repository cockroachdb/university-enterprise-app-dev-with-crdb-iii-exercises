package com.cockroachlabs.university.contention;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ContentionSimulation {

    private static String dbUrl;
    private static String dbUsername;
    private static String dbPassword;

    private static Jdbi jdbi;
    private static HikariDataSource ds;

    // Application options
    private static int uniqueIds;
    private static int numberOfWorkers;
    private static int seconds;
    private static boolean isSelectForUpdate;

    private static List<Integer> ids = new CopyOnWriteArrayList<>();

    private static AtomicInteger retryErrors = new AtomicInteger(0);
    private static AtomicInteger totalNumberTransactions = new AtomicInteger(0);
    
    public static void main(String[] args) {

        setApplicationOptions(args);
        setupDatabasePool();
        setupDatabase();

        // Start the simulation
        ExecutorService executor = Executors.newFixedThreadPool(numberOfWorkers);
        long endTime = System.currentTimeMillis() + (seconds * 1000);

        log.info("Starting simulation...");

        while (System.currentTimeMillis() < endTime) {
            executor.submit(() -> {
                Random random = new Random();
                int randomId = ids.get(random.nextInt(numberOfWorkers));

                try (Handle handle = jdbi.open()) {
                    handle.begin();

                    if(isSelectForUpdate){
                        String value = handle.createQuery("SELECT value FROM contend WHERE id = :id FOR UPDATE")
                            .bind("id", randomId)
                            .mapTo(String.class)
                            .one();
                    } else {
                        String value = handle.createQuery("SELECT value FROM contend WHERE id = :id")
                            .bind("id", randomId)
                            .mapTo(String.class)
                            .one();
                    }
                    
                    handle.createUpdate("UPDATE contend SET value = :value WHERE id = :id")
                            .bind("id", randomId)
                            .bind("value", "newValue" + randomId)
                            .execute();

                    handle.commit();
                    totalNumberTransactions.incrementAndGet();
                } catch (Exception e) {
                    log.error("Exception caught "+ e.getMessage());
                    if(e.getMessage().contains("WriteTooOldError")){
                        retryErrors.incrementAndGet();
                    }
                }
            });
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                
                Thread.currentThread().interrupt();
            }
        }

        executor.shutdown();
        try {
            if(!executor.awaitTermination(seconds+2, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            };
        } catch (InterruptedException e) {
            e.printStackTrace();
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        tearDownDatabase();

        log.info("THE TOTAL NUMBER OF TRANSACTIONS: " + totalNumberTransactions.get());
        log.info("THE NUMBER OF RETRY ERRORS: " + retryErrors.get());
    }

    private static void setApplicationOptions(String[] args) {
        Options options = new Options();

        Option uniqueIdsOption = new Option("u", "uniqueIds", true, "Number of unique IDs");
        uniqueIdsOption.setRequired(true);
        options.addOption(uniqueIdsOption);

        Option workersOption = new Option("w", "numberOfWorkers", true, "Number of workers");
        workersOption.setRequired(true);
        options.addOption(workersOption);

        Option secondsOption = new Option("s", "seconds", true, "Number of seconds to run");
        secondsOption.setRequired(true);
        options.addOption(secondsOption);

        Option selectForUpdateOption = new Option(null, "selectForUpdate", false, "Use SELECT FOR UPDATE in transactions");
        options.addOption(selectForUpdateOption);

        Option setPriorityOption = new Option(null, "setPriority", false, "Add high priority option to a selection of transactions");
        options.addOption(setPriorityOption);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("ContentionSimulation", options);
            System.exit(1);
        }

        uniqueIds = Integer.parseInt(cmd.getOptionValue("uniqueIds"));
        numberOfWorkers = Integer.parseInt(cmd.getOptionValue("numberOfWorkers"));
        seconds = Integer.parseInt(cmd.getOptionValue("seconds"));
        isSelectForUpdate = cmd.hasOption("selectForUpdate");
        isSetPriority = cmd.hasOption("setPriority");
    }

    private static void setupDatabasePool() {
        try {
            File configFile = new File("target/application.properties");

            Properties properties = new Properties();
            try (FileInputStream input = new FileInputStream(configFile)) {
                properties.load(input);
            }

            dbUrl = properties.getProperty("db.url");
            dbUsername = properties.getProperty("db.username");
            dbPassword = properties.getProperty("db.password");

        } catch (IOException exception) {
            log.error(exception.getMessage(), exception);
            System.exit(1);
        }

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(dbUrl);
        config.setUsername(dbUsername);
        config.setPassword(dbPassword);
        config.setMaximumPoolSize(20); 
        ds = new HikariDataSource(config);
        
        // Wrap the HikariDataSource in JDBI:
        jdbi = Jdbi.create(ds);
    }

    private static void setupDatabase() {
        jdbi.useHandle(handle -> {
            handle.execute("CREATE TABLE IF NOT EXISTS contend (id INT PRIMARY KEY, value STRING)");
            handle.execute("DELETE FROM contend");

            IntStream.rangeClosed(1, uniqueIds).forEach(id -> {
                handle.createUpdate("INSERT INTO contend (id, value) VALUES (:id, :value)")
                        .bind("id", id)
                        .bind("value", "Value" + id)
                        .execute();

                ids.add(id);
            });
        });
    }

    private static void insertRows(int count) {
        jdbi.useHandle(handle -> {
            IntStream.rangeClosed(1, count).forEach(id -> {
                handle.createUpdate("INSERT INTO contend (id, value) VALUES (:id, :value)")
                        .bind("id", id)
                        .bind("value", "Value" + id)
                        .execute();

                ids.add(id);
            });
        });
    }

    private static void tearDownDatabase() {
        jdbi.useHandle(handle -> {
            handle.execute("DROP TABLE contend");
        });

        // Close the connection pool
        ds.close();
    }
}
