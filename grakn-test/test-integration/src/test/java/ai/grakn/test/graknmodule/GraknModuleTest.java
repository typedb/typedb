package ai.grakn.test.graknmodule;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import ai.grakn.engine.module.GraknModuleManager;
import ai.grakn.graknmodule.GraknModule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;

public class GraknModuleTest {
    private final String DUMMY_MODULE_NAME = "dummy-module";
    private final String DUMMY_MODULE_JAR_URL = "https://github.com/lolski/grakn-dummy-module/raw/master/dummy-module-1.0-SNAPSHOT.jar";
    private final String DUMMY_MODULE_DIR = "./modules/dummy-module";

    @Test
    public void dummyModule_ShouldBeLoadedWhenFoundInModulesDirectory() {
        String name = withDummyModule(() -> {
            GraknModuleManager.initialise();
            List<GraknModule> modules = GraknModuleManager.getGraknModules();
            GraknModule dummyModule = modules.get(0);
            return dummyModule.getGraknModuleName();
        });

        assertEquals(name, DUMMY_MODULE_NAME);
    }

    private <T> T withDummyModule(Supplier<T> fn) {
        mkdir(DUMMY_MODULE_DIR);
        downloadFile(DUMMY_MODULE_JAR_URL, DUMMY_MODULE_DIR);
        T result = fn.get();
        rmdir(DUMMY_MODULE_DIR);
        return result;
    }

    private void mkdir(String dir) {
        // create destination directory if not exists
        try {
            FileUtils.forceMkdir(new File(dir));
        } catch (IOException e) {
            throw new RuntimeException("Unable to force create directory '" + dir + "'", e);
        }

    }

    private void rmdir(String dir) {
        // create destination directory if not exists
        try {
            FileUtils.forceDelete(new File(dir));
        } catch (IOException e) {
            throw new RuntimeException("Unable to force delete directory '" + dir + "'", e);
        }
    }
    private void downloadFile(String sourceUrl, String destDir) {
        // download the file
        try {
            String jarName = FilenameUtils.getName(sourceUrl);
            String destination = destDir + "/" + jarName;
            File dest = new File(destination);
            FileUtils.copyURLToFile(new URL(sourceUrl), dest);
        } catch (IOException e) {
            throw new RuntimeException(
                "Unable to download dummy module test fixture file from '" + sourceUrl + "'", e);
        }

    }
}
