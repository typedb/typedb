package ai.grakn.engine.lock;

import ai.grakn.util.EmbeddedRedis;
import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisLockTest {

    private static final int PORT = 7001;
    public static final String LOCK_NAME = "LOCK_NAME";
    private static JedisPool jedisPool;

    @BeforeClass
    public static void setupClass() {
        EmbeddedRedis.start(PORT);
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        jedisPool = new JedisPool(poolConfig, "localhost", PORT);
    }

    @AfterClass
    public static void tearDownClass() throws InterruptedException {
        jedisPool.close();
        EmbeddedRedis.stop();

    }

    @Test
    public void whenOtherThreadHasLock_TryLockFails() throws Exception {
        Lock lock = new JedisLock(jedisPool, LOCK_NAME);
        lock.lock();

        Thread t = new Thread(() -> {
            Lock lock1 = new JedisLock(jedisPool, LOCK_NAME);
            Assert.assertFalse(lock1.tryLock());
        });

        t.start();
        t.join();
        lock.unlock();

        Thread t2 = new Thread(() -> {
            Lock lock1 = new JedisLock(jedisPool, LOCK_NAME);
            Assert.assertTrue(lock1.tryLock());
        });

        t2.start();
        t2.join();
    }

    @Test
    public void whenOtherThreadHasLock_TryLockTimesOut() throws Exception {
        Lock lock = new JedisLock(jedisPool, LOCK_NAME);
        lock.lock();

        Thread t = new Thread(() -> {
            Lock lock1 = new JedisLock(jedisPool, LOCK_NAME);
            try {
                Stopwatch stopwatch = Stopwatch.createStarted();
                int time = 5000;
                Assert.assertFalse(lock1.tryLock(time, TimeUnit.MILLISECONDS));
                Assert.assertTrue(stopwatch.stop().elapsed(TimeUnit.MILLISECONDS) > time/2 /*Leaving some margin*/);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        t.start();
        t.join();
        lock.unlock();
    }
}