package grakn.core.cli;

public class GraknComponentLoaderException extends RuntimeException {

    public GraknComponentLoaderException(String message) {
        super(message);
    }

    public GraknComponentLoaderException(Throwable cause) {
        super(cause);
    }
}
