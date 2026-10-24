package com.novanovastudio.dto;

import java.util.Map;

/**
 * 图片模型的分辨率分档计费配置。
 *
 * @param resolutionPrices Map<String, Integer> 1K、2K、4K 到积分单价的映射，空表示沿用模型单价
 * @author zhenglin.cn.cq@gmail.com
 * @date 2026-10-21 00:00
 */
public record ImageBillingConfiguration(
        Map<String, Integer> resolutionPrices
) {
}
