package grakn.core.common.async;

import grakn.core.common.iterator.ResourceIterator;

public class BaseProducer<T> implements Producer<T> {

    private ResourceIterator<T> iterator;

    BaseProducer(ResourceIterator<T> iterator) {
        this.iterator = iterator;
    }

    @Override
    public void produce(Sink<T> sink, int count) {
        for (int i = 0; i < count; i++) {
            if (iterator.hasNext()) {
                sink.put(iterator.next());
            } else {
                sink.done();
                break;
            }
        }
    }

    @Override
    public void recycle() {
        iterator.recycle();
    }
}
