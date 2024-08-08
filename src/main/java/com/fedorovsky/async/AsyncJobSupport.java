package com.fedorovsky.async;

import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;

public class AsyncJobSupport {

    public static <T> AsyncJob<T> of(Stream<T> stream) {
        BlockingQueueProcessor<T> producer = new BlockingQueueProcessor<T>(q -> stream.forEach(it -> putEach(it, q)));
        return new AsyncJobStream<>(producer).capacity(100);
    }

    static <T> void putEach(T value, BlockingQueue<T> q) {
        try {
            q.put(value);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static class AsyncJobStream<T> extends AsyncJob<T> {

        public AsyncJobStream(BlockingQueueProcessor<T> pr) {
            super(pr);
        }

        @Override
        public AsyncJob<T> threads(int threads) {
            throw new UnsupportedOperationException("The number of threads is defined by java.util.stream.Stream");
        }
    }

}
