package grakn.core.common.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

public class NamedThreadFactory implements ThreadFactory {

    private final AtomicLong index = new AtomicLong(0);
    private final String prefix;

    public NamedThreadFactory(Class<?> clazz, String function) {
        this.prefix = clazz.getSimpleName() + "::" + function + "::";
    }

    public static NamedThreadFactory create(Class<?> clazz, String function) {
        return new NamedThreadFactory(clazz, function);
    }

    @Override
    public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setName(prefix + index.getAndIncrement());
        return thread;
    }
}
