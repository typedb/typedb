package grakn.core.cli;

/**
 * A definition of a Grakn component, containing all the information necessary to load it.
 *
 * The componentJarName is the name of the jar containing the commandClass.
 */
public class GraknComponentDefinition {
    private final String componentName;
    private final String commandClass;
    private final String componentJarName;

    public GraknComponentDefinition(String componentName,
                                    String commandClass,
                                    String componentJarName) {
        this.componentName = componentName;
        this.commandClass = commandClass;
        this.componentJarName = componentJarName;
    }

    public String getComponentName() {
        return componentName;
    }

    public String getCommandClass() {
        return commandClass;
    }

    public String getComponentJarName() {
        return componentJarName;
    }
}
