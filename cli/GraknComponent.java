package grakn.core.cli;

import picocli.CommandLine;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Represents a Grakn component available in the CLI that will be lazily loaded.
 *
 * In order to cut down on overall jars loaded, we only load the jar that contains the jar we need.
 * This class knows some parts of how the jars are laid out but GraknComponentLoader does the bulk
 * of that work.
 *
 * When we fully load this component, we notify our parent GraknComponentLoader to unload all other
 * components.
 */
public class GraknComponent {

    public enum Status {
        NOT_LOADED,
        PARTIALLY_LOADED,
        FULLY_LOADED,
        FAILED
    }

    private final GraknComponentDefinition definition;
    private final GraknComponentLoader loader;

    private CommandLine commandLine;
    private GraknClassLoader classLoader;

    private Status status = Status.NOT_LOADED;

    public GraknComponent(GraknComponentLoader loader,
                          GraknComponentDefinition definition) {
        this.loader = loader;
        this.definition = definition;
    }

    public String getName() {
        return definition.getComponentName();
    }

    public String getJarName() {
        return definition.getComponentJarName();
    }

    public CommandLine getCommand() {
        return commandLine;
    }

    /**
     * Load the command for this component.
     */
    public void partiallyLoad() {
        if (status == Status.NOT_LOADED || status == Status.FAILED) {
            classLoader = new GraknClassLoader(
                    loader.getCommandJarURL(this),
                    loader.getConfURL(this)
            );
            try {
                Class<?> clazz = classLoader.loadClass(definition.getCommandClass());
                Method method = clazz.getDeclaredMethod("buildCommand");
                Object result = method.invoke(null);
                if (result instanceof CommandLine) {
                    commandLine = (CommandLine) result;
                } else {
                    throw new GraknComponentLoaderException("buildCommand method did not return a CommandLine");
                }

                commandLine.setCommandName(definition.getComponentName());
            } catch (GraknComponentLoaderException e) {
                status = Status.FAILED;
                throw e;
            } catch (Exception e) {
                status = Status.FAILED;
                throw new GraknComponentLoaderException(e);
            }

            status = Status.PARTIALLY_LOADED;
        }
    }

    /**
     * Load this component fully and unload others.
     */
    public void fullyLoad() {
        if (status == Status.FULLY_LOADED) {
            return;
        }

        if (status == Status.NOT_LOADED || status == Status.FAILED) {
            partiallyLoad();
        }

        classLoader.addURLs(loader.getAllServiceJarURLs(this).toArray(URL[]::new));
        loader.unloadAllExcept(this);
        status = Status.FULLY_LOADED;
    }

    /**
     * Unload this component.
     */
    public void unload() {
        try {
            classLoader.close();
        } catch (IOException e) {
            throw new GraknComponentLoaderException(e);
        }
        status = Status.NOT_LOADED;
    }

    /**
     * Our own extended ClassLoaded which allows us to add URLs after we are ready to complete loading.
     */
    private static class GraknClassLoader extends URLClassLoader {

        public GraknClassLoader(URL... urls) {
            super(urls, getSystemClassLoader());
        }

        private void addURLs(URL[] urls) {
            for (URL url : urls) {
                super.addURL(url);
            }
        }
    }
}
