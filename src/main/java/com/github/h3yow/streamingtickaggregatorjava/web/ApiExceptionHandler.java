package com.github.h3yow.streamingtickaggregatorjava.web;

import com.github.h3yow.streamingtickaggregatorjava.service.RejectedTickException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 将接入拒绝映射为可区分的 422 响应：body 中携带稳定的 reason 枚举，调用方可据此区分拒绝原因。
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(RejectedTickException.class)
    public ResponseEntity<Map<String, Object>> handleRejected(RejectedTickException ex) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("accepted", false);
        body.put("duplicate", false);
        body.put("reason", ex.getReason());
        body.put("message", ex.getMessage());
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(body);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> handleUnreadable(HttpMessageNotReadableException ex) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("accepted", false);
        body.put("duplicate", false);
        body.put("reason", "MALFORMED_REQUEST");
        body.put("message", "请求体不是合法 JSON 或类型不匹配: " + ex.getMostSpecificCause().getMessage());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(body);
    }
}
