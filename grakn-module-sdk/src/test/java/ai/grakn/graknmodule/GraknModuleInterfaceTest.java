package ai.grakn.graknmodule;


import ai.grakn.graknmodule.http.HttpBeforeFilter;
import ai.grakn.graknmodule.http.HttpBeforeFilterResult;
import ai.grakn.graknmodule.http.HttpEndpoint;
import ai.grakn.graknmodule.http.HttpMethods;
import ai.grakn.graknmodule.http.HttpRequest;
import ai.grakn.graknmodule.http.HttpResponse;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static ai.grakn.graknmodule.TestModuleConstants.TEST_MODULE_ENDPOINT;
import static ai.grakn.graknmodule.TestModuleConstants.TEST_MODULE_ENDPOINT_RESPONSE;
import static ai.grakn.graknmodule.TestModuleConstants.TEST_MODULE_NAME;
import static ai.grakn.graknmodule.TestModuleConstants.TEST_MODULE_PATTERN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class GraknModuleInterfaceTest {

    @Test
    public void testModule_shouldImplementGraknModuleInterfaceProperly() {
        GraknModule testModule = new TestModule();
        assertThat(testModule.getGraknModuleName(), equalTo(TEST_MODULE_NAME));
        assertThat(testModule.getHttpBeforeFilters().get(0).getUrlPattern(), equalTo(TEST_MODULE_PATTERN));
        assertThat(testModule.getHttpEndpoints().get(0).getEndpoint(), equalTo(TEST_MODULE_ENDPOINT));
    }
}

class TestModuleConstants {
    public static final String TEST_MODULE_NAME = "test-module";
    public static final String TEST_MODULE_PATTERN = "test-pattern";
    public static final String TEST_MODULE_ENDPOINT = "test-endpoint";
    public static final String TEST_MODULE_ENDPOINT_RESPONSE = "test-response";
}

class TestModule implements GraknModule {
    public String getGraknModuleName() {
        return TEST_MODULE_NAME;
    }

    public List<HttpBeforeFilter> getHttpBeforeFilters() {
        return Arrays.asList(beforeFilter1);
    }

    public List<HttpEndpoint> getHttpEndpoints() {
        return Arrays.asList(httpPostEndpoint);
    }

    private HttpBeforeFilter beforeFilter1 = new HttpBeforeFilter() {
        @Override
        public String getUrlPattern() {
            return TEST_MODULE_PATTERN;
        }

        @Override
        public HttpBeforeFilterResult getHttpBeforeFilter(HttpRequest request) {
            return HttpBeforeFilterResult.allowRequest();
        }
    };

    private HttpEndpoint httpPostEndpoint = new HttpEndpoint() {
        @Override
        public HttpMethods.HTTP_METHOD getHttpMethod() {
            return HttpMethods.HTTP_METHOD.POST;
        }

        @Override
        public String getEndpoint() {
            return TEST_MODULE_ENDPOINT;
        }

        @Override
        public HttpResponse getRequestHandler(HttpRequest request) {
            return new HttpResponse(200, TEST_MODULE_ENDPOINT_RESPONSE);
        }
    };
}