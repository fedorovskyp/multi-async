package com.fedorovsky.async;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

public class AsyncJob<T> {

    final BlockingQueueProcessor<T> pr;
    private BlockingQueueProcessor.AsyncJobConfig cfg = new BlockingQueueProcessor.AsyncJobConfig(1, 100, null);

    public AsyncJob(BlockingQueueProcessor<T> pr) {
        this.pr = pr;
    }

    public AsyncJob<T> capacity(int capacity) {
        cfg = new BlockingQueueProcessor.AsyncJobConfig(cfg.threads(), capacity, cfg.onError());
        return this;
    }

    public AsyncJob<T> threads(int threads) {
        cfg = new BlockingQueueProcessor.AsyncJobConfig(threads, cfg.capacity(), cfg.onError());
        return this;
    }

    public AsyncJob<T> exceptionally(Consumer<Throwable> handler) {
        cfg = new BlockingQueueProcessor.AsyncJobConfig(cfg.threads(), cfg.capacity(), handler);
        return this;
    }

    public <OUTPUT> AsyncJob<OUTPUT> andThen(Function<T, OUTPUT> converter) {
        var consumer = new BlockingQueueProcessor<>(Objects.requireNonNull(converter), this);
        return new AsyncJobInternal<>(this, consumer);
    }

    public <OUT> List<OUT> consume(Function<T, OUT> converter) {
        this.start();
        return pr.reduceAndJoinSilent(new ArrayList<>(), (a, b) -> {
            a.add(converter.apply(b));
            return a;
        });
    }

    public void start() {
        Objects.requireNonNull(pr).start(cfg);
    }

    private static class AsyncJobInternal<T, K> extends AsyncJob<T> {
        private AsyncJob<K> main;

        public AsyncJobInternal(AsyncJob<K> main, BlockingQueueProcessor<T> nextJob) {
            super(nextJob);
            this.main = main;
        }

        @Override
        public void start() {
            this.main.start();
            super.start();
        }

        @Override
        public AsyncJob<T> exceptionally(Consumer<Throwable> handler) {
            if (main.cfg.onError() == null) {
                main.cfg = new BlockingQueueProcessor.AsyncJobConfig(main.cfg.threads(), main.cfg.capacity(), handler);
            }
            return super.exceptionally(handler);
        }
    }

}