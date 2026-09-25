package com.github.h3yow.streamingtickaggregatorjava.tests;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

/**
 * HTTP 协议集成测试：接入 200/422、幂等重复、查询与迟到归类端点。
 */
@SpringBootTest(properties = {
        "aggregator.window-size-ms=60000",
        "aggregator.allowed-lateness-ms=10000"
})
@AutoConfigureMockMvc
class WebApiIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(WebApiIntegrationTest.class);

    // 取整到分钟，保证窗口起点可复现；且为过去时间以通过 eventTime<=now 校验
    private static final long BASE = (System.currentTimeMillis() / 60_000) * 60_000 - 600_000;

    @Autowired
    private MockMvc mvc;

    @Test
    void endToEndAcceptDuplicateRejectLateAndQuery() throws Exception {
        // 1) 正常接入
        String tick1 = """
                {"eventId":"w-1","symbol":"NVDA","sequence":1,"eventTime":%d,"price":100.5,"volume":10}
                """.formatted(BASE + 1_000);
        MvcResult r1 = mvc.perform(post("/api/ticks").contentType(MediaType.APPLICATION_JSON).content(tick1))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accepted").value(true))
                .andExpect(jsonPath("$.duplicate").value(false))
                .andExpect(jsonPath("$.windowStatus").value("OPEN"))
                .andReturn();
        log.info("[HTTP-ACCEPT] body={}", r1.getResponse().getContentAsString());

        // 2) 重复投递：仍 200，duplicate=true
        MvcResult r2 = mvc.perform(post("/api/ticks").contentType(MediaType.APPLICATION_JSON).content(tick1))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accepted").value(true))
                .andExpect(jsonPath("$.duplicate").value(true))
                .andReturn();
        log.info("[HTTP-DUP] body={}", r2.getResponse().getContentAsString());

        // 3) 缺字段：422 + 可区分 reason
        String missing = """
                {"eventId":"w-bad","symbol":"NVDA","sequence":2,"eventTime":%d,"volume":1}
                """.formatted(BASE + 2_000);
        mvc.perform(post("/api/ticks").contentType(MediaType.APPLICATION_JSON).content(missing))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("MISSING_FIELD"));

        // 4) 推进 watermark 关闭窗口
        String advance = """
                {"eventId":"w-adv","symbol":"NVDA","sequence":3,"eventTime":%d,"price":101,"volume":1}
                """.formatted(BASE + 71_000);
        mvc.perform(post("/api/ticks").contentType(MediaType.APPLICATION_JSON).content(advance))
                .andExpect(status().isOk());

        // 5) 超时迟到：422 + WINDOW_CLOSED_LATE，且进入 /api/late 归类
        String late = """
                {"eventId":"w-late","symbol":"NVDA","sequence":4,"eventTime":%d,"price":500,"volume":99}
                """.formatted(BASE + 5_000);
        MvcResult rLate = mvc.perform(post("/api/ticks").contentType(MediaType.APPLICATION_JSON).content(late))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("WINDOW_CLOSED_LATE"))
                .andReturn();
        log.info("[HTTP-LATE] body={}", rLate.getResponse().getContentAsString());

        // 6) 查询已确认窗口（默认不含 OPEN）；迟到事件未污染
        MvcResult q = mvc.perform(get("/api/ohlcv").param("symbol", "NVDA"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].status").value("CLOSED"))
                .andExpect(jsonPath("$[0].high").value(100.5))
                .andExpect(jsonPath("$[0].volume").value(10))
                .andExpect(jsonPath("$[0].tickCount").value(1))
                .andReturn();
        log.info("[HTTP-QUERY] body={}", q.getResponse().getContentAsString());

        // 7) 迟到归类端点
        mvc.perform(get("/api/late").param("symbol", "NVDA"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].eventId").value("w-late"))
                .andExpect(jsonPath("$[0].reason").value("WINDOW_CLOSED_LATE"));

        // 8) includeOpen=true 可额外看到未关闭窗口
        mvc.perform(get("/api/ohlcv").param("symbol", "NVDA").param("includeOpen", "true"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));

        // 9) 非法 JSON：400 MALFORMED_REQUEST
        mvc.perform(post("/api/ticks").contentType(MediaType.APPLICATION_JSON).content("{not-json"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.reason").value("MALFORMED_REQUEST"));
    }
}
