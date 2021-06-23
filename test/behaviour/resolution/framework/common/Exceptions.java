package com.vaticle.typedb.core.test.behaviour.resolution.framework.common;

public class Exceptions {

    public static class SoundnessException extends RuntimeException {
        public SoundnessException(String message) {
            super(message);
        }
    }

    public static class CompletenessException extends RuntimeException {
        CompletenessException(String message) {
            super(message);
        }
    }

    public static class ResolutionTestingException extends RuntimeException {
        public ResolutionTestingException(String message) {
            super(message);
        }
    }
}
