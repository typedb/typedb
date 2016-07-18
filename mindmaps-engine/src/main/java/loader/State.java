package loader;

/**
 * The set of states of graph loader.
 *
 * @author sheldon
 */
public enum State {
    QUEUED, LOADING, FINISHED, ERROR, CANCELLED
}
