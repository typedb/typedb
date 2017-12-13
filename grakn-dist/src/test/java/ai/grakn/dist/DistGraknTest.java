package ai.grakn.dist;

import ai.grakn.dist.lock.Lock;
import ai.grakn.dist.lock.LockAlreadyAcquiredException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.mockito.Mockito.*;

public class DistGraknTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void graknServerStart_shouldAttemptToStartAllComponents() {
        String[] command = {"server", "start" };

        StorageProcess storageProcess = mock(StorageProcess.class);
        QueueProcess queueProcess = mock(QueueProcess.class);
        GraknProcess graknProcess = mock(GraknProcess.class);
        Lock synchronizedBootupLock = newLock_whichAcquiresLockSuccessfully();
        DistGrakn distGrakn = new DistGrakn(storageProcess, queueProcess, graknProcess, synchronizedBootupLock);
        distGrakn.run(command);

        verify(storageProcess).start();
        verify(queueProcess).start();
        verify(graknProcess).start();
    }

    @Test
    public void graknServerStart_shouldAttemptToStartStorageComponent() {
        String[] command = { "server", "start", "storage" };

        StorageProcess storageProcess = mock(StorageProcess.class);
        QueueProcess queueProcess = mock(QueueProcess.class);
        GraknProcess graknProcess = mock(GraknProcess.class);
        Lock synchronizedBootupLock = newLock_whichAcquiresLockSuccessfully();
        DistGrakn distGrakn = new DistGrakn(storageProcess, queueProcess, graknProcess, synchronizedBootupLock);
        distGrakn.run(command);

        verify(storageProcess).start();
        verify(queueProcess, never()).start();
        verify(graknProcess, never()).start();
    }

    @Test
    public void graknServerStart_shouldAttemptToStartQueueComponent() {
        String[] command = { "server", "start", "queue" };

        StorageProcess storageProcess = mock(StorageProcess.class);
        QueueProcess queueProcess = mock(QueueProcess.class);
        GraknProcess graknProcess = mock(GraknProcess.class);
        Lock synchronizedBootupLock = newLock_whichAcquiresLockSuccessfully();
        DistGrakn distGrakn = new DistGrakn(storageProcess, queueProcess, graknProcess, synchronizedBootupLock);
        distGrakn.run(command);

        verify(storageProcess, never()).start();
        verify(queueProcess).start();
        verify(graknProcess, never()).start();
    }

    @Test
    public void graknServerStart_shouldAttemptToStartGraknEngineComponent() {
        String[] command = { "server", "start", "grakn" };

        StorageProcess storageProcess = mock(StorageProcess.class);
        QueueProcess queueProcess = mock(QueueProcess.class);
        GraknProcess graknProcess = mock(GraknProcess.class);
        Lock synchronizedBootupLock = newLock_whichAcquiresLockSuccessfully();
        DistGrakn distGrakn = new DistGrakn(storageProcess, queueProcess, graknProcess, synchronizedBootupLock);
        distGrakn.run(command);

        verify(storageProcess, never()).start();
        verify(queueProcess, never()).start();
        verify(graknProcess).start();
    }

    @Test
    public void graknServerStop_shouldAttemptToStopAllComponents() {
        String[] command = { "server", "stop" };

        StorageProcess storageProcess = mock(StorageProcess.class);
        QueueProcess queueProcess = mock(QueueProcess.class);
        GraknProcess graknProcess = mock(GraknProcess.class);
        Lock synchronizedBootupLock = newLock_whichAcquiresLockSuccessfully();
        DistGrakn distGrakn = new DistGrakn(storageProcess, queueProcess, graknProcess, synchronizedBootupLock);
        distGrakn.run(command);

        verify(storageProcess).stop();
        verify(queueProcess).stop();
        verify(graknProcess).stop();
    }

    @Test
    public void graknServerStart_shouldFailToStartAllComponents_ifLockIsAlreadyAcquired()
            throws LockAlreadyAcquiredException {

        String[] command = {"server", "start" };

        StorageProcess storageProcess = mock(StorageProcess.class);
        QueueProcess queueProcess = mock(QueueProcess.class);
        GraknProcess graknProcess = mock(GraknProcess.class);
        Lock synchronizedBootupLock = newLock_whichThrowsLockAlreadyAcquiredException();
        DistGrakn distGrakn = new DistGrakn(storageProcess, queueProcess, graknProcess, synchronizedBootupLock);

        expectedException.expect(LockAlreadyAcquiredException.class);
        distGrakn.run(command);
    }

    @Test
    public void graknServerStart_shouldFailToStopAllComponents_ifLockIsAlreadyAcquired()
            throws LockAlreadyAcquiredException {

        String[] command = {"server", "stop" };

        StorageProcess storageProcess = mock(StorageProcess.class);
        QueueProcess queueProcess = mock(QueueProcess.class);
        GraknProcess graknProcess = mock(GraknProcess.class);
        Lock synchronizedBootupLock = newLock_whichThrowsLockAlreadyAcquiredException();
        DistGrakn distGrakn = new DistGrakn(storageProcess, queueProcess, graknProcess, synchronizedBootupLock);

        expectedException.expect(LockAlreadyAcquiredException.class);
        distGrakn.run(command);
    }

    private Lock newLock_whichAcquiresLockSuccessfully() {
        return new Lock() {
            @Override
            public void withLock(Runnable fn) {
                fn.run();
            }
        };
    }

    private Lock newLock_whichThrowsLockAlreadyAcquiredException() {
        return new Lock() {
            @Override
            public void withLock(Runnable fn) {
                throw new LockAlreadyAcquiredException();
            }
        };
    }
}