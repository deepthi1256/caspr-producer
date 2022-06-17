package com.walmart.caspr.service;


import com.walmart.caspr.model.api.ResponseBody;
import reactor.core.publisher.Mono;

import java.util.Map;

public interface CasperService {

    Mono<ResponseBody> postPayloadToCasper(String payload, String endpoint, Map<String, String> headers, Map<String, String> cookies);

}
