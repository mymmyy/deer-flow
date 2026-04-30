---
name: feishu-chart-contract
description: Enforce Feishu chart output via explicit contract metadata and avoid local script-based chart generation.
license: MIT
---

# Feishu Chart Contract

Use this skill only when the request is from Feishu channel and the user asks for chart/table visualization.

## Hard Rules

1. Do NOT execute bash/node/python scripts to generate charts.
2. Do NOT ask the user to allow script execution.
3. Output chart content through structured metadata contract:
   - Preferred: `metadata.feishu_skill_contract`
   - Compatible: `metadata.feishu_card_payload.chart_spec`
4. If contract is not possible, return clear text fallback.

## Preferred Output Shape

Return metadata with:

```json
{
  "feishu_skill_contract": {
    "card_schema_version": "v1",
    "target_channel": "feishu",
    "render_mode": "card",
    "fallback_text": "无法生成图表，已返回文本摘要。",
    "card_payload": {
      "title": "近7天花费趋势",
      "summary": "按天展示最近7天花费变化。",
      "blocks": [
        {
          "type": "chart",
          "chart": {
            "chart_type": "line",
            "dimension": {
              "name": "日期",
              "values": ["04-21", "04-22", "04-23", "04-24", "04-25", "04-26", "04-27"]
            },
            "metrics": [
              {
                "name": "花费",
                "values": [1200, 1320, 1280, 1450, 1510, 1490, 1580]
              }
            ]
          }
        }
      ]
    }
  }
}
```

## Text Fallback

When chart contract cannot be produced, respond with:

1. concise text summary of trend;
2. no script execution requests;
3. optional recommendation to retry with clearer dimension/metric fields.
