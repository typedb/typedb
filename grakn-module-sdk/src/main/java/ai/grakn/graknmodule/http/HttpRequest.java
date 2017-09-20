package ai.grakn.graknmodule.http;

import java.util.Map;
import java.util.Set;

public class HttpRequest {
    private final Map<String, String> headers;
    private final Map<String, String> queryParameters;
    private final String requestBody;

    public HttpRequest(Map<String, String> headers, Map<String, String> queryParameters, String requestBody) {
        this.headers = headers;
        this.queryParameters = queryParameters;
        this.requestBody = requestBody;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public Map<String, String> getQueryParameters() {
        return queryParameters;
    }

    public String getRequestBody() {
        return requestBody;
    }
}
