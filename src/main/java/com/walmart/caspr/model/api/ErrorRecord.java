package com.walmart.caspr.model.api;


import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
public class ErrorRecord {

    private String topicName;
    private String response;
    private String exceptionMessage;

}
