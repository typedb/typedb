package grakn.core.common.async;

import java.util.function.Predicate;

public class FilteredProducer<T> implements Producer<T> {

    private final Producer<T> baseProducer;
    private final Predicate<T> predicate;

    public FilteredProducer(Producer<T> baseProducer, Predicate<T> predicate) {
        this.baseProducer = baseProducer;
        this.predicate = predicate;
    }

    @Override
    public void produce(Producer.Sink<T> sink, int count) {
        baseProducer.produce(new Sink(sink), count);
    }

    @Override
    public void recycle() {
        baseProducer.recycle();
    }

    private class Sink implements Producer.Sink<T> {

        private final Producer.Sink<T> baseSink;

        Sink(Producer.Sink<T> baseSink) {
            this.baseSink = baseSink;
        }

        @Override
        public void put(T item) {
            if (predicate.test(item)) baseSink.put(item);
            else baseProducer.produce(this, 1);
        }

        @Override
        public void done() {
            baseSink.done();
        }
    }
}
