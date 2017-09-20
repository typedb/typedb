package ai.grakn.graknmodule.http;

public class Response {
    public Response(int statusCode, String body) {
        this.statusCode = statusCode;
        this.body = body;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getBody() {
        return body;
    }

    private final int statusCode;
    private final String body;
}
