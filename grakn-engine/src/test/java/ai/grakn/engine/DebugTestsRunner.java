package ai.grakn.engine;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import ai.grakn.engine.config.GraknEngineConfigTest;
import ai.grakn.engine.controller.ConceptControllerTest;
import ai.grakn.engine.controller.GraqlControllerDeleteTest;
import ai.grakn.engine.controller.GraqlControllerInsertTest;
import ai.grakn.engine.lock.ProcessWideLockProvider;

/**
 * Run a selected set of classes/methods as a main program. Helps with debugging tests, especially
 * troubleshooting interdependencies...
 * 
 * @author borislav
 *
 */
public class DebugTestsRunner {

    public static void main(String[] argv) {
        Class<?> [] torun = new Class[] { 
                GraknEngineConfigTest.class,
                ConceptControllerTest.class,
                GraqlControllerDeleteTest.class,
                ProcessWideLockProvider.class,
                GraqlControllerInsertTest.class};
        JUnitCore junit = new JUnitCore();
        Result result = null;
        do {
            result = junit.run(torun);
        } while (result.getFailureCount() == 0 && false);
        System.out.println("Failures " + result.getFailureCount());
        if (result.getFailureCount() > 0) {
            for (Failure failure : result.getFailures()) {
                failure.getException().printStackTrace();
            }
        }
        System.exit(0);
    }
}