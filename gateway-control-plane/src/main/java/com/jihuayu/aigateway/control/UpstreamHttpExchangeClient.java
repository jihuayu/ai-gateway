package com.jihuayu.aigateway.control;

import java.util.Map;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.annotation.GetExchange;
import org.springframework.web.service.annotation.HttpExchange;
import org.springframework.web.service.annotation.PostExchange;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

/**
 * Declarative upstream provider client powered by Spring RestClient & @HttpExchange.
 * Designed for synchronous virtual-thread execution without RestTemplate or OpenFeign.
 */
@HttpExchange(accept = MediaType.APPLICATION_JSON_VALUE)
public interface UpstreamHttpExchangeClient {

    @GetExchange("/models")
    Map<String, Object> listModels(@RequestHeader("Authorization") String authorization);

    @PostExchange(value = "/chat/completions", contentType = MediaType.APPLICATION_JSON_VALUE)
    Map<String, Object> chatCompletions(
        @RequestHeader("Authorization") String authorization,
        @RequestBody Map<String, Object> request
    );

    /**
     * Creates a declarative {@link UpstreamHttpExchangeClient} bound to the given upstream base URL
     * using Spring's synchronous {@link RestClient}.
     */
    static UpstreamHttpExchangeClient create(RestClient.Builder builder, String baseUrl) {
        RestClient restClient = builder.baseUrl(baseUrl).build();
        RestClientAdapter adapter = RestClientAdapter.create(restClient);
        HttpServiceProxyFactory factory = HttpServiceProxyFactory.builderFor(adapter).build();
        return factory.createClient(UpstreamHttpExchangeClient.class);
    }
}
