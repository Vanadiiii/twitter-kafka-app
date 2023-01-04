package me.imatveev.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class BookLineReader {
    private final File book;
    private final ExecutorService executorService;

    private volatile boolean closed;

    public BookLineReader(File book) {
        this.book = book;
        this.executorService = Executors.newSingleThreadExecutor();
        this.closed = false;
    }

    public void doForEveryLineWithDelay(Consumer<String> lineConsumer,
                                        long delay,
                                        TimeUnit unit) {
        executorService.submit(
                () -> {
                    try (final BufferedReader reader = new BufferedReader(new FileReader(book))) {

                        String line;
                        while ((line = reader.readLine()) != null && !closed) {
                            if (!line.isBlank()) {
                                lineConsumer.accept(line);
                                try {
                                    unit.sleep(delay);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
        );
    }

    public void stop() {
        this.closed = true;
        executorService.shutdownNow();
    }

    public boolean isStopped() {
        return closed || executorService.isShutdown();
    }
}
