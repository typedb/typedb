package ai.grakn.engine.tasks.manager.redisqueue;

import ai.grakn.util.EmbeddedRedis;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static junit.framework.TestCase.fail;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.client.Client;
import net.greghaines.jesque.client.ClientPoolImpl;
import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;
import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerEvent;
import net.greghaines.jesque.worker.WorkerPoolImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisTest {

    private static final Config config = new ConfigBuilder().build();
    public static final int PORT = 9899;


    @Before
    public void setUp() {
        EmbeddedRedis.start(PORT);
    }

    @After
    public void tearDown() {
        EmbeddedRedis.stop();
    }

    @Test
    public void testJesque() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(16);
        JedisPool jedisPool = new JedisPool(poolConfig, "localhost", PORT);
        final Job job = new Job("TestAction", 1, 2.3, true, "test", Arrays.asList("inner", 4.5));
        final Client client = new ClientPoolImpl(config, jedisPool);
        client.enqueue("foo", job);
        client.end();

        final Worker worker = new WorkerPoolImpl(config,
                Collections.singletonList("foo"), new MapBasedJobFactory(map(entry("TestAction", TestAction.class))), jedisPool);

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        worker.getWorkerEventEmitter().addListener(
                (event, worker1, queue, job1, runner, result, t) -> {
                    if (runner instanceof RedisTaskQueueConsumer) {
                        countDownLatch.countDown();
                    }
                }, WorkerEvent.JOB_SUCCESS);

        final Thread workerThread = new Thread(worker);
        workerThread.start();
        try {
            countDownLatch.await(3, TimeUnit.SECONDS);
            worker.end(true);
            workerThread.join();
        } catch (Exception e) {
            fail();
        }
    }

    public static class TestAction  implements  Runnable{
        private static final Logger LOG = LoggerFactory.getLogger(TestAction.class);

        private final Integer i;
        private final Double d;
        private final Boolean b;
        private final String s;
        private final List<Object> l;

        public TestAction(final Integer i, final Double d, final Boolean b, final String s, final List<Object> l) {
            this.i = i;
            this.d = d;
            this.b = b;
            this.s = s;
            this.l = l;
        }

        public void run() {
            LOG.info("TestAction.run() {} {} {} {} {}", this.i, this.d, this.b, this.s, this.l);
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                LOG.error("Failed to run test action", e);
            }
        }
    }
}