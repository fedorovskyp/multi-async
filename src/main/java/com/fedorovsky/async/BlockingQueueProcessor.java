package com.fedorovsky.async;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

final class BlockingQueueProcessor<T> {

    static final Object SIGNAL_END = new Object();
    BlockingQueue<T> sharedQueue;
    private final InterruptableConsumer<BlockingQueue<T>> sharedQueueConsumer;

    public BlockingQueueProcessor(InterruptableConsumer<BlockingQueue<T>> sharedQueueConsumer) {
        this.sharedQueueConsumer = sharedQueueConsumer;
    }

    public <O> BlockingQueueProcessor(Function<O, T> converter, AsyncJob<O> job) {
        this(createInterruptableConsumer(converter, job));
    }

    public static <T, O> InterruptableConsumer<BlockingQueue<T>> createInterruptableConsumer(Function<O, T> converter, AsyncJob<O> job) {
        return q -> {
            while (true) {
                var next = job.pr.sharedQueue.take();

                if (SIGNAL_END.equals(next)) {
                    putSignalEnd(job.pr.sharedQueue); // put signal back to notify the other threads
                    return;
                }
                var convertedNext = next == null ? null : converter.apply((O) next);

                q.put(convertedNext);
            }
        };
    }

	// public static <T, R> InterruptableConsumer<BlockingQueue<R>> createInterruptableConsumer(int batchSize, BlockingQueue<T> queue, Function<List<T>, R> converter) {
	// 	return q -> {
	// 		List<T> holder = new ArrayList<>();
	// 		BlockingQueue<Object> genericQ = (BlockingQueue<Object>) queue;
	// 		while (true) {
	// 			var next = genericQ.take();

	// 			if (SIGNAL_END.equals(next)) {
	// 				q.put(converter.apply(holder));
	// 				putSignalEnd(genericQ); // put signal back to notify other threads
	// 				return;
	// 			}

	// 			holder.add((T) next);

	// 			if (holder.size() >= batchSize) {
	// 				q.put(converter.apply(holder));
	// 				holder = new ArrayList<>(holder.size());
	// 			}
	// 		}
	// 	};
	// }


    @SuppressWarnings({"rawtypes", "unchecked"})
    static <T> void putSignalEnd(BlockingQueue source) throws InterruptedException {
        source.put(SIGNAL_END);
    }

    private static <T> void putSignalEndSafe(BlockingQueue<T> source) {
        try {
            putSignalEnd(source);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void start() {
        start(new AsyncJobConfig(1, 100, null));
    }

    public void start(AsyncJobConfig cfg) {
        if (this.sharedQueue != null) {
            throw new AsyncJobException("Processor can be started only once");
        }
        if (cfg.threads <= 0) {
            throw new AsyncJobException("Must provide at least 1 thread");
        }
        if (cfg.capacity <= 0) {
            throw new AsyncJobException("Capacity must be bigger than 0");
        }

        this.sharedQueue = new LinkedBlockingQueue<>(cfg.capacity);
        var taskExecutor = Executors.newFixedThreadPool(cfg.threads);

        var futures = IntStream
                .range(0, cfg.threads).boxed()
                .map(x -> CompletableFuture.runAsync(new BlockingQueueProcessorThread<>(this.sharedQueue, this.sharedQueueConsumer), taskExecutor))
                .toArray(CompletableFuture[]::new);


        CompletableFuture.allOf(futures)
                .thenAccept(x -> putSignalEndSafe(this.sharedQueue))
                .exceptionally(x -> {
                    try {
                        if (cfg.onError != null) {
                            cfg.onError.accept(x);
                        }
                    } finally {
                        putSignalEndSafe(this.sharedQueue);
                    }
                    return null;
                });


        taskExecutor.shutdown();
    }

    /**
     * Blocks a thread until the whole queue is consumed
     **/
    public <R> R reduceAndJoin(R identity, BiFunction<R, T, R> function) throws InterruptedException {
        R result = identity;
        while (true) {
            var next = this.sharedQueue.take();
            if (next.equals(SIGNAL_END)) {
                return result;
            }
            result = function.apply(result, next);
        }
    }

    public <R> R reduceAndJoinSilent(R identity, BiFunction<R, T, R> function) {
        try {
            return reduceAndJoin(identity, function);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return identity;
        }
    }

    @FunctionalInterface
    public interface InterruptableConsumer<T> {
        void accept(T t) throws InterruptedException;
    }

    public static class BlockingQueueProcessorThread<T> extends Thread {

        private final BlockingQueue<T> sharedQueue;
        private final InterruptableConsumer<BlockingQueue<T>> sharedQueueConsumer;

        public BlockingQueueProcessorThread(BlockingQueue<T> sharedQueue, InterruptableConsumer<BlockingQueue<T>> sharedQueueConsumer) {
            this.sharedQueue = sharedQueue;
            this.sharedQueueConsumer = sharedQueueConsumer;
        }

        @Override
        public void run() {
            try {
                sharedQueueConsumer.accept(this.sharedQueue);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static record AsyncJobConfig(int threads, int capacity, Consumer<Throwable> onError) {
    }

}
