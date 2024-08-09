package com.fedorovsky.async;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.Test;

public class AsyncJobTest {

    @Test
    public void AsyncJobSupport_DoesntConsume() {
        var stream = Stream.of(1, 2, 3);

        AsyncJobSupport.of(stream);

        assertDoesNotThrow(stream::spliterator);
    }

    @Test
    public void AsyncJobSupport_Consumes() {
        var stream = Stream.of(1, 2, 3);

        AsyncJobSupport.of(stream).consume(Function.identity());

        assertThrows(IllegalStateException.class, stream::spliterator);
    }

    @Test
    public void exceptionInUpstream_stops() throws Exception {
        var iterator = new Iterator<Integer>() {

            private AtomicInteger counter = new AtomicInteger(1);

            @Override
            public boolean hasNext() {
                if (counter.get() == 3) {
                    throw new RuntimeException("The 3rd element throws exception");
                }
                return counter.get() < 5;
            }

            @Override
            public Integer next() {
                return counter.getAndIncrement();
            }

        };
        var stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);

        var job = AsyncJobSupport.of(stream);
        var out = job.consume(Function.identity());

        assertTrue(job.pr.sharedQueue.isEmpty());
        assertEquals(List.of(1, 2), out);
    }

    @Test
    public void exceptionInDownstream() {
        var stream = Stream.of(1, 2, 3, 4, 5);
        var counter = new AtomicInteger();

        var out = AsyncJobSupport.of(stream).andThen(next -> {
            var el = counter.incrementAndGet();
            if (el == 3) {
                throw new RuntimeException("Exception in a 3rd element");
            }
            return next;
        }).consume(Function.identity());

        assertEquals(List.of(1, 2), out);
    }

    @Test
    public void exceptionally_Downstream() {
        var stream = Stream.of(1, 2, 3, 4, 5);
        var counter = new AtomicInteger();
        var holder = new TestUtils.Holder<Throwable>();

        var out = AsyncJobSupport.of(stream).andThen(next -> {
            var el = counter.incrementAndGet();
            if (el == 3) {
                throw new RuntimeException("Exception processing the 3rd element");
            }
            return next;
        }).exceptionally(ex -> {
            holder.set(ex);
        }).consume(Function.identity());

        assertEquals(List.of(1, 2), out);
        var exception = holder.get();
        assertNotNull(exception);
        assertEquals("Exception processing the 3rd element", exception.getCause().getMessage());
    }

    @Test
    public void exceptionally_SharesHandler() {
        var stream = Stream.of(1, 2, 3, 4, 5);
        var counter = new AtomicInteger();
        var holder = new TestUtils.Holder<Throwable>();

        var out = AsyncJobSupport.of(stream).andThen(next -> {
            var el = counter.incrementAndGet();
            if (el == 3) {
                throw new RuntimeException("Exception processing the 3rd element");
            }
            return next;
        }).andThen(i -> i * 2).exceptionally(ex -> {
            holder.set(ex);
        }).consume(Function.identity());

        assertEquals(List.of(2, 4), out);
        var exception = holder.get();
        assertNotNull(exception);
        assertEquals("Exception processing the 3rd element", exception.getCause().getMessage());
    }

    @Test
    public void exceptionally_OwnHandler() {
        var stream = Stream.of(1, 2, 3, 4, 5);
        var counter = new AtomicInteger();
        var holder = new TestUtils.Holder<Throwable>();
        var holder2 = new TestUtils.Holder<Throwable>();

        var out = AsyncJobSupport.of(stream).andThen(next -> {
            var el = counter.incrementAndGet();
            if (el == 3) {
                throw new RuntimeException("Exception processing the 3rd element");
            }
            return next;
        }).exceptionally(holder::set).andThen(i -> {
            if (i == 1) {
                return 100;
            }
            throw new RuntimeException("Child process failed");
        }).exceptionally(holder2::set).consume(Function.identity());

        assertEquals(List.of(100), out);
        var exception = holder.get();
        assertNotNull(exception);
        assertEquals("Exception processing the 3rd element", exception.getCause().getMessage());
        exception = holder2.get();
        assertNotNull(exception);
        assertEquals("Child process failed", exception.getCause().getMessage());
    }

    @Test
    public void startTwice_throws() {
        var stream = Stream.of(1, 2, 3);
        var job = AsyncJobSupport.of(stream);
        job.consume(Function.identity());
        assertThrows(AsyncJobException.class, () -> job.consume(Function.identity()));
    }
}
