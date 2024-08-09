package com.fedorovsky.async;

public class TestUtils {
    
    public static class Holder<T> {
        public T value;

        public Holder() {}
    
        public Holder(T value) {
            this.value = value;
        }

        public void set(T v) {
            this.value = v;
        }

        public T get() {
            return this.value;
        }
    }
}
