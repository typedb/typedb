package ai.grakn.dist;

import org.junit.Test;

import static org.mockito.Mockito.*;

public class DistGraknTest {
    @Test
    public void graknServerStart_shouldAttemptToStartAllComponents() {
        String[] command = {"server", "start" };

        StorageProcess storageProcess = mock(StorageProcess.class);
        QueueProcess queueProcess = mock(QueueProcess.class);
        GraknProcess graknProcess = mock(GraknProcess.class);
        DistGrakn distGrakn = new DistGrakn(storageProcess, queueProcess, graknProcess);
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
        DistGrakn distGrakn = new DistGrakn(storageProcess, queueProcess, graknProcess);
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
        DistGrakn distGrakn = new DistGrakn(storageProcess, queueProcess, graknProcess);
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
        DistGrakn distGrakn = new DistGrakn(storageProcess, queueProcess, graknProcess);
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
        DistGrakn distGrakn = new DistGrakn(storageProcess, queueProcess, graknProcess);
        distGrakn.run(command);

        verify(storageProcess).stop();
        verify(queueProcess).stop();
        verify(graknProcess).stop();
    }
}
