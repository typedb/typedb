package grakn.core.cli;

import picocli.CommandLine;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

public class GraknComponentLoader {

    private String commandClass;
    private URL[] mainUrls;
    private URL[] dependencyUrls;

    private GraknClassLoader classLoader;

    public GraknComponentLoader(String commandClass, URL[] mainUrls, URL[] otherUrls) {
        this.commandClass = commandClass;
        this.mainUrls = mainUrls;
        dependencyUrls = otherUrls;
    }

    public CommandLine loadCommand() throws Exception {
        classLoader = new GraknClassLoader(mainUrls);
        Class<?> clazz = classLoader.loadClass(commandClass);
        Method method = clazz.getDeclaredMethod("buildCommand");
        Object result = method.invoke(null);
        if (result instanceof CommandLine) {
            return (CommandLine) result;
        } else {
            throw new Exception("buildCommand method did not return a CommandLine");
        }
    }

    public void addAllDeps() {
        classLoader.addURLs(dependencyUrls);
    }

    public void close() throws Exception {
        classLoader.close();
    }

    private static class GraknClassLoader extends URLClassLoader {

        public GraknClassLoader(URL[] urls) {
            super(urls, getSystemClassLoader());
        }

        private void addURLs(URL[] urls) {
            for (URL url : urls) {
                super.addURL(url);
            }
        }
    }
}
