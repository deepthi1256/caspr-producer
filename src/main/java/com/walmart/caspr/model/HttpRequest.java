package com.walmart.caspr.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class HttpRequest {
    @JsonProperty("url")
    private String url;
    @JsonProperty("method")
    private String method;
    @JsonProperty("headers")
    private Map<String, Object> headers;
    @JsonProperty("cookies")
    private Map<String, Object> cookies;
    @JsonProperty("payLoad")
    private String payLoad;
    @JsonProperty("exception")
    private String exception;
    @JsonProperty("validator")
    private Validator validator;
}
