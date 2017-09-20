package ai.grakn.test.graknmodule;

import ai.grakn.engine.module.GraknModuleManager;
import ai.grakn.graknmodule.GraknModule;
import ai.grakn.graknmodule.http.HttpEndpoint;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class GraknModuleTest {
    @Test
    public void aGraknModule_ShouldBeLoadedWhenLocatedInModulesDirectory() {
        GraknModuleManager.initialise();
        List<GraknModule> modules = GraknModuleManager.getGraknModules();
        GraknModule dummyModule = modules.get(0);
        String name = dummyModule.getGraknModuleName();
        assertEquals(name, "dummy-module");
    }
}
