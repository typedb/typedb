package hypergraph.common.concurrent;

import java.util.concurrent.CountDownLatch;

public class ReadWriteMutex<TYPE> {

    CountDownLatch readersLatch;
    CountDownLatch writerLatch;

    public ReadWriteMutex() {

    }

    public void lockRead(TYPE reader) throws InterruptedException {
        writerLatch.await();
        synchronized (this) {
            readersLatch = new CountDownLatch(1);
        }

    }

    public void lockWrite(TYPE writer) {



        writerLatch = new CountDownLatch(1);
    }
}
