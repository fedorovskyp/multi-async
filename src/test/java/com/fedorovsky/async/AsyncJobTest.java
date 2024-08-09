package com.fedorovsky.async;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    public void AsyncJobSupport_ExceptionInUpstream() {
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

        var out = AsyncJobSupport.of(stream).consume(Function.identity());

        assertEquals(List.of(1, 2), out);
    }

    @Test
    public void AsyncJobSupport_ExceptionInDownstream() {
        var stream = Stream.of(1, 2, 3, 4, 5);
        var counter = new AtomicInteger();

        var out = AsyncJobSupport.of(stream)
            .andThen(next -> {
                var el = counter.incrementAndGet();
                if (el == 3) {
                    throw new RuntimeException("Exception in a 3rd element");
                }
                return next;
            })
            .consume(Function.identity());

        assertEquals(List.of(1, 2), out);
    }
}
