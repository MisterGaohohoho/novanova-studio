package com.novanovastudio.ai.provider;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.novanovastudio.agent.AgentScopeModelFactory;
import com.novanovastudio.ai.AiHttpClient;
import com.novanovastudio.ai.AiMediaSupport;
import com.novanovastudio.ai.AiTaskExecutionContext;
import com.novanovastudio.common.BusinessException;
import com.novanovastudio.config.NovanovaProperties;
import com.novanovastudio.dto.AiTaskDtos;
import com.novanovastudio.dto.PersistenceDtos;
import com.novanovastudio.entity.AiGenerationTask;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.Model;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * 自定义模型参考素材完整性与最终请求参数测试。
 *
 * @author zhenglin.cn.cq@gmail.com
 * @date 2026-10-16 10:00
 */
class CustomProviderAdapterTest {

    /** 参考图片地址。 */
    private static final String IMAGE_URL = "https://example.com/reference.png";

    /** 含签名参数的参考视频地址。 */
    private static final String VIDEO_URL = "https://example.com/reference.mp4?token=a%2Fb&expires=123";

    /** 参考音频地址。 */
    private static final String AUDIO_URL = "https://example.com/reference.mp3";

    /** 用于捕获最终请求的HTTP客户端。 */
    private AiHttpClient aiHttpClient;

    /** 构造请求体的文本模型。 */
    private Model model;

    /** 待测试的自定义模型适配器。 */
    private CustomProviderAdapter adapter;

    /**
     * 初始化模拟依赖，在请求发送边界停止执行以检查最终参数。
     *
     * @return void 无返回值
     */
    @BeforeEach
    void setUp() {
        aiHttpClient = mock(AiHttpClient.class);
        AiMediaSupport mediaSupport = mock(AiMediaSupport.class);
        model = mock(Model.class);
        AgentScopeModelFactory modelFactory = mock(AgentScopeModelFactory.class);
        when(modelFactory.defaultTextModel()).thenReturn(Mono.just(model));
        when(mediaSupport.resolveReferenceUrl(eq(1L), any(AiTaskDtos.AiTaskMediaReference.class)))
                .thenAnswer(invocation -> Mono.just(invocation.<AiTaskDtos.AiTaskMediaReference>getArgument(1).url()));
        when(aiHttpClient.sendBearerJsonUrlRequest(any(), eq("POST"), eq("https://provider.example/generate"), any()))
                .thenReturn(Mono.error(new IllegalStateException("测试已捕获请求")));
        adapter = new CustomProviderAdapter(aiHttpClient, mediaSupport, new NovanovaProperties());
        ReflectionTestUtils.setField(adapter, "agentScopeModelFactory", modelFactory);
    }

    /**
     * AI首次遗漏视频后应收到遗漏原因，并在修正后将全部素材原样提交到嵌套参数。
     *
     * @return void 无返回值
     */
    @Test
    void shouldCorrectMissingVideoBeforeSubmittingAllReferences() {
        String completeBody = JSON.toJSONString(Map.of("prompt", "人物在奔跑", "input", Map.of(
                "image", IMAGE_URL, "motion", List.of(Map.of("url", VIDEO_URL)), "audio", List.of(AUDIO_URL))));
        givenAgentOutputs(JSON.toJSONString(Map.of("image", IMAGE_URL, "audio", AUDIO_URL)), completeBody);
        AiTaskExecutionContext context = videoContext("{\"input\":{\"motion\":{{videoReferences}}}}", "按请求示例映射素材字段",
                List.of(IMAGE_URL), List.of(VIDEO_URL), List.of(AUDIO_URL));

        StepVerifier.create(adapter.execute(context)).expectErrorMessage("测试已捕获请求").verify(Duration.ofSeconds(5));

        JSONObject body = submittedBody();
        Assertions.assertEquals(IMAGE_URL, body.getJSONObject("input").getString("image"));
        Assertions.assertEquals(VIDEO_URL, body.getJSONObject("input").getJSONArray("motion").getJSONObject(0).getString("url"));
        Assertions.assertEquals(AUDIO_URL, body.getJSONObject("input").getJSONArray("audio").getString(0));
        verify(model, times(2)).stream(anyList(), anyList(), any(GenerateOptions.class));
        verify(model).stream(argThat(messages -> messages.getFirst().getTextContent().contains("上次构造错误")
                        && messages.getFirst().getTextContent().contains(VIDEO_URL)), anyList(), any(GenerateOptions.class));
        verify(model, times(2)).stream(argThat(messages -> messages.getFirst().getTextContent().contains("请求示例")
                        && messages.getFirst().getTextContent().contains("motion")), anyList(), any(GenerateOptions.class));
    }

    /**
     * AI持续遗漏视频时必须停止提交，保留具体遗漏信息而非返回重试耗尽的通用错误。
     *
     * @return void 无返回值
     */
    @Test
    void shouldRejectMissingVideoWithoutSendingUpstreamRequest() {
        givenAgentOutputs(JSON.toJSONString(Map.of("prompt", "人物在奔跑", "ref_image_0", IMAGE_URL)));
        AiTaskExecutionContext context = videoContext("", "只配置了图片字段",
                List.of(IMAGE_URL), List.of(VIDEO_URL), List.of());

        StepVerifier.create(adapter.execute(context)).expectErrorSatisfies(exception -> {
            Assertions.assertInstanceOf(BusinessException.class, exception);
            Assertions.assertTrue(exception.getMessage().contains("请求体遗漏参考素材URL"));
            Assertions.assertTrue(exception.getMessage().contains(VIDEO_URL));
            Assertions.assertFalse(exception.getMessage().contains("Retries exhausted"));
        }).verify(Duration.ofSeconds(5));

        verify(model, times(3)).stream(anyList(), anyList(), any(GenerateOptions.class));
        verifyNoInteractions(aiHttpClient);
    }

    /**
     * 仅将视频URL写进提示词不能视为已传递参考视频参数。
     *
     * @return void 无返回值
     */
    @Test
    void shouldRejectVideoUrlOnlyIncludedInPrompt() {
        givenAgentOutputs(JSON.toJSONString(Map.of("prompt", VIDEO_URL)));
        AiTaskExecutionContext context = videoContext("", "构造视频请求", List.of(), List.of(VIDEO_URL), List.of());

        StepVerifier.create(adapter.execute(context))
                .expectErrorSatisfies(exception -> Assertions.assertTrue(exception.getMessage().contains("请求体遗漏参考素材URL")))
                .verify(Duration.ofSeconds(5));

        verifyNoInteractions(aiHttpClient);
    }

    /**
     * 多段视频必须逐一传入，不能因请求体已有一段视频就忽略其他视频。
     *
     * @return void 无返回值
     */
    @Test
    void shouldRejectPartiallySubmittedVideoReferences() {
        String secondVideoUrl = "https://example.com/second.mp4";
        givenAgentOutputs(JSON.toJSONString(Map.of("videos", List.of(VIDEO_URL))));
        AiTaskExecutionContext context = videoContext("", "构造视频请求",
                List.of(), List.of(VIDEO_URL, secondVideoUrl), List.of());

        StepVerifier.create(adapter.execute(context))
                .expectErrorSatisfies(exception -> Assertions.assertTrue(exception.getMessage().contains(secondVideoUrl)))
                .verify(Duration.ofSeconds(5));

        verifyNoInteractions(aiHttpClient);
    }

    /**
     * 请求模板应通过既有视频占位符将视频URL数组原样提交，且不调用AI构造。
     *
     * @return void 无返回值
     */
    @Test
    void shouldSubmitVideoReferencesFromTemplate() {
        AiTaskExecutionContext context = videoContext("{\"prompt\":{{prompt}},\"input\":{\"motion\":{{videoReferences}}}}", "",
                List.of(), List.of(VIDEO_URL), List.of());

        StepVerifier.create(adapter.execute(context)).expectErrorMessage("测试已捕获请求").verify(Duration.ofSeconds(5));

        Assertions.assertEquals(VIDEO_URL, submittedBody().getJSONObject("input").getJSONArray("motion").getString(0));
        verifyNoInteractions(model);
    }

    /**
     * 模板未引用视频时应在提交前明确报错，不擅自插入未知的协议字段。
     *
     * @return void 无返回值
     */
    @Test
    void shouldRejectTemplateWithoutVideoMapping() {
        AiTaskExecutionContext context = videoContext("{\"prompt\":{{prompt}}}", "", List.of(), List.of(VIDEO_URL), List.of());

        StepVerifier.create(adapter.execute(context))
                .expectErrorSatisfies(exception -> Assertions.assertTrue(exception.getMessage().contains(VIDEO_URL)))
                .verify(Duration.ofSeconds(5));

        verifyNoInteractions(aiHttpClient, model);
    }

    /**
     * 素材完整性校验也应发现图片和音频遗漏，避免只检查视频。
     *
     * @return void 无返回值
     */
    @Test
    void shouldValidateImageAndAudioReferencesAlongsideVideo() {
        givenAgentOutputs(JSON.toJSONString(Map.of("videos", List.of(VIDEO_URL))));
        AiTaskExecutionContext context = videoContext("", "按实际素材构造请求",
                List.of(IMAGE_URL), List.of(VIDEO_URL), List.of(AUDIO_URL));

        StepVerifier.create(adapter.execute(context)).expectErrorSatisfies(exception -> {
            Assertions.assertTrue(exception.getMessage().contains(IMAGE_URL));
            Assertions.assertTrue(exception.getMessage().contains(AUDIO_URL));
        }).verify(Duration.ofSeconds(5));

        verifyNoInteractions(aiHttpClient);
    }

    /**
     * 未携带参考素材的请求应照常提交，并保留展示模型与实际接口模型的独立配置。
     *
     * @return void 无返回值
     */
    @Test
    void shouldKeepRequestsWithoutReferencesAndSeparateModelNames() {
        givenAgentOutputs("{\"prompt\":\"人物在奔跑\"}");
        AiTaskExecutionContext context = videoContext("", "根据用户输入构造请求", List.of(), List.of(), List.of());

        StepVerifier.create(adapter.execute(context)).expectErrorMessage("测试已捕获请求").verify(Duration.ofSeconds(5));

        Assertions.assertEquals("人物在奔跑", submittedBody().getString("prompt"));
        verify(model).stream(argThat(messages -> messages.getFirst().getTextContent().contains("minimax_h3_image_audio_to_video_v2")),
                anyList(), any(GenerateOptions.class));
    }

    /**
     * 自定义图片能力均应按模型开关替换size，同时保留模板写死的size。
     *
     * @param conversion boolean 是否转换为像素
     * @param editing boolean 是否为图生图
     * @param custom boolean 是否使用模型独立的尺寸映射
     */
    @ParameterizedTest
    @CsvSource({"false,false,false", "true,false,false", "false,true,false", "true,true,false",
            "false,false,true", "true,false,true", "false,true,true", "true,true,true"})
    void shouldResolveTemplateSizeWithoutChangingOriginalRatio(boolean conversion, boolean editing, boolean custom) {
        // 图生图请求必须把参考图写进请求体，否则适配器会在发请求前拦截。
        String template = editing
                ? "{\"size\":\"{{size}}\",\"resolution\":\"{{resolution}}\",\"image\":\"https://provider.example/reference.png\"}"
                : "{\"size\":\"{{size}}\",\"resolution\":\"{{resolution}}\"}";
        AiTaskExecutionContext context = imageContext(conversion, editing, template, null,
                custom ? Map.of("9:16", Map.of("2K", "1440x2560")) : null);

        StepVerifier.create(adapter.execute(context)).expectErrorMessage("测试已捕获请求").verify(Duration.ofSeconds(5));

        JSONObject body = submittedBody();
        Assertions.assertEquals(conversion ? custom ? "1440x2560" : "1152x2048" : "9:16", body.getString("size"));
        Assertions.assertEquals("2K", body.getString("resolution"));
        Assertions.assertEquals("9:16", context.request().parameters().get("size"));
        verifyNoInteractions(model);
    }

    /**
     * 模板未引用size时无需查表，手写值与省略size均不受缺失映射影响。
     *
     * @param editing boolean 是否为图生图
     * @param fixedSize boolean 模板是否写死size
     */
    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void shouldSkipConversionForTemplateWithoutSizePlaceholder(boolean editing, boolean fixedSize) {
        // 图生图请求必须把参考图写进请求体，否则适配器会在发请求前拦截。
        String template = fixedSize ? "{\"size\":\"1024x1024\"" : "{\"prompt\":\"{{prompt}}\"";
        template += editing ? ",\"image\":\"https://provider.example/reference.png\"}" : "}";

        StepVerifier.create(adapter.execute(imageContext(true, editing, template, null, Map.of())))
                .expectErrorMessage("测试已捕获请求").verify(Duration.ofSeconds(5));

        Assertions.assertEquals(fixedSize ? "1024x1024" : null, submittedBody().getString("size"));
        verifyNoInteractions(model);
    }

    /**
     * AI构造请求应接收原始比例、分辨率和计算好的size，不自行换算。
     *
     * @param conversion boolean 是否转换为像素
     * @param expectedSize String 应传递的请求尺寸
     * @param custom boolean 是否使用模型尺寸表
     */
    @ParameterizedTest
    @CsvSource({"true,1152x2048,false", "false,9:16,false", "true,1440x2560,true"})
    void shouldDescribeCalculatedSizeToRequestAgent(boolean conversion, String expectedSize, boolean custom) {
        givenAgentOutputs("{\"size\":\"" + expectedSize + "\"}");

        AiTaskExecutionContext context = imageContext(conversion, false, "{}", "按参数构造size",
                custom ? Map.of("9:16", Map.of("2K", "1440x2560")) : null);
        StepVerifier.create(adapter.execute(context)).expectErrorMessage("测试已捕获请求").verify(Duration.ofSeconds(5));

        Assertions.assertEquals(expectedSize, submittedBody().getString("size"));
        verify(model).stream(argThat(messages -> {
            String prompt = messages.getFirst().getTextContent();
            return prompt.contains("画幅: 9:16") && prompt.contains("分辨率: 2K")
                    && prompt.contains("请求size: " + expectedSize) && prompt.contains("不要自行换算");
        }), anyList(), any(GenerateOptions.class));
    }

    /** 选择未配置的档位时应在发起供应商请求前失败。 */
    @Test
    void shouldRejectUnconfiguredSizeBeforeSendingRequest() {
        AiTaskExecutionContext context = imageContext(true, false, "{\"size\":\"{{size}}\"}", null, Map.of());

        StepVerifier.create(adapter.execute(context))
                .expectErrorSatisfies(exception -> Assertions.assertInstanceOf(BusinessException.class, exception))
                .verify(Duration.ofSeconds(5));

        verifyNoInteractions(aiHttpClient);
    }

    /**
     * 构造用于自定义图片请求尺寸验证的执行上下文。
     *
     * @param conversion boolean 是否转换为像素
     * @param editing boolean 是否为图生图
     * @param requestTemplate String 请求模板
     * @param aiRequestPrompt String AI请求构造提示词
     * @param mappings Map<String, Map<String, String>> 模型独立的尺寸映射
     * @return AiTaskExecutionContext 自定义模型图片任务上下文
     */
    private AiTaskExecutionContext imageContext(boolean conversion, boolean editing, String requestTemplate,
                                                String aiRequestPrompt, Map<String, Map<String, String>> mappings) {
        AiGenerationTask task = new AiGenerationTask();
        task.setId("custom-image-task");
        task.setUserId(1L);
        task.setTaskType("image");
        AiTaskDtos.CreateAiTaskRequest request = new AiTaskDtos.CreateAiTaskRequest("image", "生成小猫", "custom-image",
                Map.of("size", "9:16", "resolution", "2K"),
                editing ? mediaReferences(List.of("https://provider.example/reference.png"), "image/png") : List.of(),
                List.of(), "imagePage", null, null, null, null);
        PersistenceDtos.CustomModelGroupConfig group = new PersistenceDtos.CustomModelGroupConfig("/generate", "POST",
                "custom-image", requestTemplate, aiRequestPrompt, "{}", "data.url", "", "GET", "", "", "", "");
        return new AiTaskExecutionContext(task,
                new AiTaskDtos.AiChannelConfig("custom-channel", "自定义渠道", "https://provider.example", "test-key", "custom", List.of("custom-image")),
                "custom-image", false, "", new JSONObject(), request, () -> Mono.just(false), progress -> Mono.empty(),
                delta -> Mono.empty(), true, Map.of(editing ? "image-to-image" : "text-to-image", group), conversion, mappings);
    }

    /**
     * AI把模型名改写成近似标识时，服务端应回填为配置的请求模型名称。
     *
     * @return void 无返回值
     */
    @Test
    void shouldRestoreModelNameRewrittenByAgent() {
        givenAgentOutputs("{\"model\":\"seedance-2.0-fast-to-video\",\"prompt\":\"用户提示词\"}");

        StepVerifier.create(adapter.execute(agentContext("seedance-2.0-fast-text-to-video", "{\"model\":\"{{model}}\",\"prompt\":\"{{prompt}}\"}")))
                .expectErrorMessage("测试已捕获请求").verify(Duration.ofSeconds(5));

        Assertions.assertEquals("seedance-2.0-fast-text-to-video", submittedBody().getString("model"));
    }

    /**
     * AI改写用户提示词时，服务端应回填为用户原始提示词。
     *
     * @return void 无返回值
     */
    @Test
    void shouldRestorePromptRewrittenByAgent() {
        givenAgentOutputs("{\"model\":\"seedance-2.0-fast-text-to-video\",\"prompt\":\"被改写的提示词\"}");

        StepVerifier.create(adapter.execute(agentContext("seedance-2.0-fast-text-to-video", "{\"model\":\"{{model}}\",\"prompt\":\"{{prompt}}\"}")))
                .expectErrorMessage("测试已捕获请求").verify(Duration.ofSeconds(5));

        Assertions.assertEquals("真实电影感，清晨窗光下的木质桌面", submittedBody().getString("prompt"));
    }

    /**
     * AI输出没有模型字段时不应擅自新增，避免引入用户未要求的字段。
     *
     * @return void 无返回值
     */
    @Test
    void shouldNotAddModelFieldWhenAgentOmitsIt() {
        givenAgentOutputs("{\"duration\":5}");

        StepVerifier.create(adapter.execute(agentContext("seedance-2.0-fast-text-to-video", "{\"duration\":5}")))
                .expectErrorMessage("测试已捕获请求").verify(Duration.ofSeconds(5));

        Assertions.assertFalse(submittedBody().containsKey("model"));
    }

    /**
     * 构造提示词里的模型占位符应替换为真实模型名，让模型直接照抄而无需翻译。
     *
     * @return void 无返回值
     */
    @Test
    void shouldRenderModelPlaceholderInAgentPrompt() {
        ChatResponse response = mock(ChatResponse.class);
        TextBlock text = mock(TextBlock.class);
        when(text.getText()).thenReturn("{\"model\":\"seedance-2.0-fast-text-to-video\"}");
        when(response.getContent()).thenReturn(List.of(text));
        when(model.stream(anyList(), anyList(), any(GenerateOptions.class))).thenAnswer(invocation -> {
            List<io.agentscope.core.message.Msg> messages = invocation.getArgument(0);
            String prompt = messages.getFirst().getTextContent();
            Assertions.assertTrue(prompt.contains("\"model\": \"seedance-2.0-fast-text-to-video\""));
            Assertions.assertFalse(prompt.contains("{{model}}"));
            return Flux.just(response);
        });

        StepVerifier.create(adapter.execute(agentContext("seedance-2.0-fast-text-to-video", "{}")))
                .expectErrorMessage("测试已捕获请求").verify(Duration.ofSeconds(5));
    }

    /**
     * 构造用于AI构造请求体验证的视频任务上下文。
     *
     * @param requestModelName String 配置的请求模型名称
     * @param requestTemplate String 请求示例模板
     * @return AiTaskExecutionContext 自定义模型视频任务上下文
     */
    private AiTaskExecutionContext agentContext(String requestModelName, String requestTemplate) {
        AiGenerationTask task = new AiGenerationTask();
        task.setId("custom-agent-task");
        task.setUserId(1L);
        task.setTaskType("video");
        AiTaskDtos.CreateAiTaskRequest request = new AiTaskDtos.CreateAiTaskRequest("video", "真实电影感，清晨窗光下的木质桌面", requestModelName,
                Map.of("resolution", "480p", "seconds", "5"), List.of(), List.of(), "videoPage", null, null, "text-to-video", null);
        PersistenceDtos.CustomModelGroupConfig group = new PersistenceDtos.CustomModelGroupConfig("/generate", "POST",
                requestModelName, requestTemplate, "model固定不变：{\"model\": \"{{model}}\"}", "{}", "data.url", "", "GET", "", "", "", "");
        return new AiTaskExecutionContext(task,
                new AiTaskDtos.AiChannelConfig("custom-channel", "自定义渠道", "https://provider.example", "test-key", "custom", List.of(requestModelName)),
                requestModelName, false, "", new JSONObject(), request, () -> Mono.just(false), progress -> Mono.empty(),
                delta -> Mono.empty(), true, Map.of("text-to-video", group), false, null);
    }

    /**
     * 配置模型流式响应，多个输出依次用于重试，最后一个输出可持续返回。
     *
     * @param outputs String[] AI构造的JSON文本
     */
    private void givenAgentOutputs(String... outputs) {
        List<Flux<ChatResponse>> responseStreams = new ArrayList<>();
        for (String output : outputs) {
            ChatResponse response = mock(ChatResponse.class);
            TextBlock text = mock(TextBlock.class);
            when(text.getText()).thenReturn(output);
            when(response.getContent()).thenReturn(List.of(text));
            responseStreams.add(Flux.just(response));
        }
        var responses = when(model.stream(anyList(), anyList(), any(GenerateOptions.class)));
        for (Flux<ChatResponse> responseStream : responseStreams) {
            responses = responses.thenReturn(responseStream);
        }
    }

    /**
     * 提取实际发送到HTTP客户端的唯一请求体。
     *
     * @return JSONObject 最终提交的请求体
     */
    private JSONObject submittedBody() {
        ArgumentCaptor<JSONObject> body = ArgumentCaptor.forClass(JSONObject.class);
        verify(aiHttpClient).sendBearerJsonUrlRequest(any(), eq("POST"), eq("https://provider.example/generate"), body.capture());
        return body.getValue();
    }

    /**
     * 构造自定义全能参考视频任务，分别设置各类素材。
     *
     * @param requestTemplate String 请求模板
     * @param aiRequestPrompt String AI构造提示词
     * @param imageUrls List<String> 参考图片URL
     * @param videoUrls List<String> 参考视频URL
     * @param audioUrls List<String> 参考音频URL
     * @return AiTaskExecutionContext 自定义模型任务上下文
     */
    private AiTaskExecutionContext videoContext(String requestTemplate, String aiRequestPrompt,
                                                List<String> imageUrls, List<String> videoUrls, List<String> audioUrls) {
        AiGenerationTask task = new AiGenerationTask();
        task.setId("custom-video-task");
        task.setUserId(1L);
        task.setTaskType("video");
        AiTaskDtos.CreateAiTaskRequest request = new AiTaskDtos.CreateAiTaskRequest("video", "人物在奔跑", "minimax_h3_lightx2v_v5",
                Map.of("size", "16:9", "seconds", 10), mediaReferences(imageUrls, "image/png"), mediaReferences(videoUrls, "video/mp4"),
                "canvas", null, null, "reference-to-video", mediaReferences(audioUrls, "audio/mpeg"));
        PersistenceDtos.CustomModelGroupConfig group = new PersistenceDtos.CustomModelGroupConfig("/generate", "POST",
                "minimax_h3_image_audio_to_video_v2", requestTemplate, aiRequestPrompt, "", "url", "", "GET", "", "", "", "");
        return new AiTaskExecutionContext(task,
                new AiTaskDtos.AiChannelConfig("custom-channel", "自定义渠道", "https://provider.example", "test-key", "custom", List.of(request.model())),
                request.model(), false, "", new JSONObject(), request, () -> Mono.just(false), progress -> Mono.empty(),
                delta -> Mono.empty(), true, Map.of("reference-to-video", group), false, null);
    }

    /**
     * 将地址列表转换为对应类型的任务参考素材。
     *
     * @param urls List<String> 素材URL列表
     * @param mimeType String 素材媒体类型
     * @return List<AiTaskMediaReference> 任务参考素材
     */
    private List<AiTaskDtos.AiTaskMediaReference> mediaReferences(List<String> urls, String mimeType) {
        return urls.stream().map(url -> new AiTaskDtos.AiTaskMediaReference(url, "参考素材", mimeType, null, url)).toList();
    }
}
