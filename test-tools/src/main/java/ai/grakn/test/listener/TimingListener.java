package ai.grakn.test.listener;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import org.junit.runner.Description;
import org.junit.runner.notification.RunListener;

public class TimingListener extends RunListener {
    
    private long startTime;
    private String test;
    
    public static void main(String [] argv)
    {
        try {
            Files.write(Paths.get("/Users/borislav/delme.txt"), Collections.singleton("Hello"), APPEND, CREATE);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    private void writeTime(String test, long time) {
        String timeLogFile = System.getProperty("grakn.test.timerecord.file");
        if (timeLogFile == null)
            timeLogFile = "grakn-test-timings.log";
        try {
            Files.write(Paths.get(timeLogFile), Collections.singleton(test + "," + time), APPEND, CREATE);
            System.out.println("WROTE to file " + Paths.get(timeLogFile));
        } catch (IOException e) {
            System.err.println("Failed to write test time to file " + timeLogFile);
            System.err.println("Please fix configuration or disable test time measurement altogether.");
            e.printStackTrace();
        }
    }
    
    public void testStarted(Description description) throws Exception {
        if (!description.isTest()) {
            return;
        }
        startTime = System.currentTimeMillis();
        test = description.getClassName() + "." + description.getMethodName();
    }

    public void testFinished(Description description) throws Exception {        
        String finishedTest = description.getClassName() + "." + description.getMethodName();
        if (!test.equals(finishedTest))
            throw new RuntimeException("Test started " + test + " different from test finished " + finishedTest);            
        writeTime(test, (System.currentTimeMillis() - startTime));
    }
}
