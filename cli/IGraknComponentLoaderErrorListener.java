package grakn.core.cli;

/**
 * We use a specialized interface in case we later want more error types and handling.
 */
public interface IGraknComponentLoaderErrorListener {
    void onError(GraknComponentLoaderException e);
}
