package com.walmart.caspr.service.impl;

import com.walmart.caspr.config.WebClientConfig;
import com.walmart.caspr.exception.CasprAPIException;
import com.walmart.caspr.model.api.ResponseBody;
import com.walmart.caspr.service.CasperService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.function.Tuple2;

import java.util.Map;
import java.util.Optional;

@Service
public class CasperServiceImpl implements CasperService {

    private final Logger log = Loggers.getLogger(CasperServiceImpl.class);

    @Override
    public Mono<ResponseBody> postPayloadToCasper(String payload, String endpoint, Map<String, String> headers, Map<String, String> cookies) {
        return WebClientConfig.webClient(endpoint)
                .post()
                .body(Mono.just(payload), String.class)
                .headers(httpHeaders -> Optional.ofNullable(headers).ifPresent(httpHeaders::setAll))
                .cookies(allCookies -> Optional.ofNullable(cookies).ifPresent(allCookies::setAll))
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .onStatus(HttpStatus::is4xxClientError, clientResponse -> {
                    throw Exceptions.propagate(new CasprAPIException("Caspr API is not reachable.."));
                })
                .onStatus(HttpStatus::is5xxServerError, clientResponse -> {
                    throw Exceptions.propagate(new CasprAPIException("Caspr API internal server error."));
                })
                .bodyToMono(ResponseBody.class)
                .elapsed()
                .doOnNext(tuple -> log.info("postPayloadToCasper:: time elapsed for Capsr API : {} ms .", tuple.getT1()))
                .map(Tuple2::getT2);
    }

}
