package com.cockroachlabs.university.batch;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.Locale;
import java.time.Duration;
import java.util.concurrent.Callable;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Timer {

    private Timer() {
    }

    public static void timeExecution(String label, Runnable task) {
        long start = System.nanoTime();
        try {
            log.info("Processing {}", label);
            task.run();
        } finally {
            long millis = Duration.ofNanos(System.nanoTime() - start).toMillis();
            log.info("{} completed in {}", label, millisecondsToDisplayString(millis));
        }
    }

    public static <V> V timeExecution(String label, Callable<V> task) {
        long start = System.nanoTime();
        try {
            log.info("Processing {}", label);
            return task.call();
        } catch (Exception e) {
            throw new UndeclaredThrowableException(e);
        } finally {
            long millis = Duration.ofNanos(System.nanoTime() - start).toMillis();
            log.info("{} completed in {}", label, millisecondsToDisplayString(millis));
        }
    }

    public static String millisecondsToDisplayString(long timeMillis) {
        if(timeMillis > 1000) {
            double seconds = (timeMillis / 1000.0) % 60;
            int minutes = (int) ((timeMillis / 60000) % 60);
            int hours = (int) ((timeMillis / 3600000));

            StringBuilder sb = new StringBuilder();
            if (hours > 0) {
                sb.append(String.format("%dh", hours));
            }
            if (hours > 0 || minutes > 0) {
                sb.append(String.format("%dm", minutes));
            }
            if (hours == 0 && seconds > 0) {
                sb.append(String.format(Locale.US, "%.3fs", seconds));
            }
            return sb.toString();
        } else
        return String.valueOf(timeMillis+"ms");
    }
}
