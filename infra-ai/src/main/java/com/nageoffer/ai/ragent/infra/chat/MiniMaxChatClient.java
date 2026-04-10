/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nageoffer.ai.ragent.infra.chat;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.nageoffer.ai.ragent.framework.convention.ChatRequest;
import com.nageoffer.ai.ragent.framework.trace.RagTraceNode;
import com.nageoffer.ai.ragent.infra.config.AIModelProperties;
import com.nageoffer.ai.ragent.infra.enums.ModelCapability;
import com.nageoffer.ai.ragent.infra.enums.ModelProvider;
import com.nageoffer.ai.ragent.infra.http.HttpMediaTypes;
import com.nageoffer.ai.ragent.infra.http.HttpResponseHelper;
import com.nageoffer.ai.ragent.infra.http.ModelClientErrorType;
import com.nageoffer.ai.ragent.infra.http.ModelClientException;
import com.nageoffer.ai.ragent.infra.http.ModelUrlResolver;
import com.nageoffer.ai.ragent.infra.model.ModelTarget;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.BufferedSource;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MiniMax 大模型提供商
 * <p>
 * MiniMax M2.7 系列通过 OpenAI 兼容协议提供 API 访问，但有以下差异：
 * <ul>
 *   <li>使用 reasoning_split=true 将思考过程分离到 reasoning_details 字段（数组格式）</li>
 *   <li>reasoning_details 格式为 [{"text": "..."}]，而非标准 reasoning_content 字符串</li>
 *   <li>不设置 reasoning_split 时，content 中会包含 &lt;think&gt; 标签</li>
 * </ul>
 */
@Slf4j
@Service
public class MiniMaxChatClient extends AbstractOpenAIStyleChatClient {

    public MiniMaxChatClient(OkHttpClient httpClient, Executor modelStreamExecutor) {
        super(httpClient, modelStreamExecutor);
    }

    @Override
    public String provider() {
        return ModelProvider.MINI_MAX.getId();
    }

    @Override
    protected void customizeRequestBody(JsonObject body, ChatRequest request) {
        // MiniMax 始终开启 reasoning_split，将思考内容分离到 reasoning_details 字段
        // 避免 content 中混入 <think...</think 标签
        body.addProperty("reasoning_split", true);
    }

    @Override
    @RagTraceNode(name = "minimax-chat", type = "LLM_PROVIDER")
    public String chat(ChatRequest request, ModelTarget target) {
        return doChat(request, target);
    }

    @Override
    @RagTraceNode(name = "minimax-stream-chat", type = "LLM_PROVIDER")
    public StreamCancellationHandle streamChat(ChatRequest request, StreamCallback callback, ModelTarget target) {
        AIModelProperties.ProviderConfig providerConfig = HttpResponseHelper.requireProvider(target, provider());
        HttpResponseHelper.requireApiKey(providerConfig, provider());

        JsonObject reqBody = buildRequestBody(request, target, true);
        Request streamRequest = new Request.Builder()
                .url(ModelUrlResolver.resolveUrl(providerConfig, target.candidate(), ModelCapability.CHAT))
                .addHeader("Authorization", "Bearer " + providerConfig.getApiKey())
                .addHeader("Accept", "text/event-stream")
                .post(RequestBody.create(reqBody.toString(), HttpMediaTypes.JSON))
                .build();

        Call call = httpClient.newCall(streamRequest);
        boolean thinkingEnabled = Boolean.TRUE.equals(request.getThinking());

        return StreamAsyncExecutor.submit(
                modelStreamExecutor,
                call,
                callback,
                cancelled -> doMiniMaxStream(call, callback, cancelled, thinkingEnabled)
        );
    }

    private void doMiniMaxStream(Call call, StreamCallback callback, AtomicBoolean cancelled, boolean thinkingEnabled) {
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                String body = HttpResponseHelper.readBody(response.body());
                throw new ModelClientException(
                        provider() + " 流式请求失败: HTTP " + response.code() + " - " + body,
                        ModelClientErrorType.fromHttpStatus(response.code()),
                        response.code()
                );
            }
            ResponseBody body = response.body();
            if (body == null) {
                throw new ModelClientException(provider() + " 流式响应为空", ModelClientErrorType.INVALID_RESPONSE, null);
            }
            BufferedSource source = body.source();
            boolean completed = false;
            while (!cancelled.get()) {
                String line = source.readUtf8Line();
                if (line == null) {
                    break;
                }
                if (line.isBlank()) {
                    continue;
                }
                try {
                    String payload = line.trim();
                    if (payload.startsWith("data:")) {
                        payload = payload.substring("data:".length()).trim();
                    }
                    if ("[DONE]".equalsIgnoreCase(payload)) {
                        callback.onComplete();
                        completed = true;
                        break;
                    }

                    JsonObject obj = gson.fromJson(payload, JsonObject.class);
                    JsonArray choices = obj.getAsJsonArray("choices");
                    if (choices == null || choices.isEmpty()) {
                        continue;
                    }
                    JsonObject choice = choices.get(0).getAsJsonObject();

                    String content = extractDeltaField(choice, "content");
                    if (thinkingEnabled) {
                        String reasoning = extractReasoningDetails(choice);
                        if (reasoning != null && !reasoning.isEmpty()) {
                            callback.onThinking(reasoning);
                        }
                    }
                    if (content != null && !content.isEmpty()) {
                        callback.onContent(content);
                    }
                    if (choice.has("finish_reason") && choice.get("finish_reason") != null && !choice.get("finish_reason").isJsonNull()) {
                        callback.onComplete();
                        completed = true;
                        break;
                    }
                } catch (Exception parseEx) {
                    log.warn("{} 流式响应解析失败: line={}", provider(), line, parseEx);
                }
            }
            if (!cancelled.get() && !completed) {
                throw new ModelClientException(provider() + " 流式响应异常结束", ModelClientErrorType.INVALID_RESPONSE, null);
            }
        } catch (Exception e) {
            callback.onError(e);
        }
    }

    private String extractDeltaField(JsonObject choice, String fieldName) {
        for (String container : new String[]{"delta", "message"}) {
            if (choice.has(container) && choice.get(container).isJsonObject()) {
                JsonObject obj = choice.getAsJsonObject(container);
                if (obj.has(fieldName)) {
                    JsonElement value = obj.get(fieldName);
                    if (value != null && !value.isJsonNull()) {
                        return value.getAsString();
                    }
                }
            }
        }
        return null;
    }

    /**
     * MiniMax 特有：从 reasoning_details 数组中提取思考内容
     * 格式：[{"text": "thinking content..."}]
     */
    private String extractReasoningDetails(JsonObject choice) {
        for (String container : new String[]{"delta", "message"}) {
            if (choice.has(container) && choice.get(container).isJsonObject()) {
                JsonObject obj = choice.getAsJsonObject(container);
                if (obj.has("reasoning_details") && obj.get("reasoning_details").isJsonArray()) {
                    JsonArray details = obj.getAsJsonArray("reasoning_details");
                    if (!details.isEmpty()) {
                        StringBuilder sb = new StringBuilder();
                        for (JsonElement elem : details) {
                            if (elem.isJsonObject()) {
                                JsonObject detail = elem.getAsJsonObject();
                                if (detail.has("text") && !detail.get("text").isJsonNull()) {
                                    sb.append(detail.get("text").getAsString());
                                }
                            }
                        }
                        if (sb.length() > 0) {
                            return sb.toString();
                        }
                    }
                }
            }
        }
        return null;
    }
}
