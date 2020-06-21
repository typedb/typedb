package grakn.core.cli;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A manager class for loading multiple components and their commands, and then discarding all but the
 * executed one when ready.
 *
 * This class also knows about the directory structure of grakn components.
 */
public class GraknComponentLoader {

    private final Path graknPath;
    private List<GraknComponent> componentList = new ArrayList<>();
    private Map<String, GraknComponent> components = new HashMap<>();

    private List<IGraknComponentLoaderErrorListener> listeners = new ArrayList<>();

    public GraknComponentLoader(Path graknPath) {
        this.graknPath = graknPath;
    }

    /**
     * Add an error listener to print errors rather than ignore them.
     *
     * @param listener An error listener.
     */
    public void addErrorListener(IGraknComponentLoaderErrorListener listener) {
        listeners.add(listener);
    }

    /**
     * Load a Grakn component. Fails silently on error but pushes errors to any error listeners.
     *
     * The order in which components are loaded is the order in which you will receive them.
     *
     * @param definition A definition of the component to load.
     */
    public void load(GraknComponentDefinition definition) {
        GraknComponent component = new GraknComponent(this, definition);
        try {
            component.partiallyLoad();
            registerComponent(component);
        } catch (GraknComponentLoaderException e) {
            listeners.forEach(l -> l.onError(e));
        }
    }

    public URL getConfURL(GraknComponent component) {
        try {
            return graknPath.resolve(Paths.get(component.getName(), "conf", "")).toUri().toURL();
        } catch (MalformedURLException e) {
            throw new GraknComponentLoaderException(e);
        }
    }

    public List<GraknComponent> getComponents() {
        return Collections.unmodifiableList(componentList);
    }

    private Path getLibPath(GraknComponent component) {
        return graknPath.resolve(Paths.get(component.getName(), "services", "lib"));
    }

    public URL getCommandJarURL(GraknComponent component) {
        try {
            return getLibPath(component).resolve(component.getJarName()).toUri().toURL();
        } catch (MalformedURLException e) {
            throw new GraknComponentLoaderException(e);
        }
    }

    public Stream<URL> getAllServiceJarURLs(GraknComponent component) {
        try {
            return Files.walk(getLibPath(component))
                    .filter(Files::isRegularFile)
                    .map(Path::toUri)
                    .map(path -> {
                        try {
                            return path.toURL();
                        } catch (MalformedURLException e) {
                            throw new GraknComponentLoaderException(e);
                        }
                    });
        } catch (IOException e) {
            throw new GraknComponentLoaderException(e);
        }
    }

    public GraknComponent getComponent(String name) {
        return components.get(name);
    }

    public void unloadAllExcept(GraknComponent component) {
        components.forEach((otherName, otherComponent) -> {
            if (!otherName.equals(component.getName())) {
                otherComponent.unload();
            }
        });
    }

    private void registerComponent(GraknComponent component) {
        componentList.add(component);
        components.put(component.getName(), component);
    }
}
