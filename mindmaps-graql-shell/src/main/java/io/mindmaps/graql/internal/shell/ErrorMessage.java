package io.mindmaps.graql.internal.shell;

@SuppressWarnings("JavaDoc")
/**
 * Error message strings for shell
 */
public enum ErrorMessage {

    COULD_NOT_CREATE_TEMP_FILE("WARNING: could not create temporary file for editing queries");

    private final String message;

    /**
     * @param message the error message string, with parameters defined using %s
     */
    ErrorMessage(String message) {
        this.message = message;
    }

    /**
     * @param args arguments to substitute into the message
     * @return the error message string, with arguments substituted
     */
    public String getMessage(Object... args) {
        return String.format(message, args);
    }
}
