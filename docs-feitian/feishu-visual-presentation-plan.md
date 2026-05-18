# 飞书深度分析可视化输出落地方案

## 1. 背景与问题

### 1.1 当前实现现状

当前飞书链路已经具备以下能力：

- `feishu_skill_contract` 结构化协议
- `feishu_card_payload.chart_spec` 兼容协议
- `feishu_contract.py` 的 schema 校验
- `feishu.py` 的 contract 渲染
- `markdown/table/chart` block 的基础渲染
- `image_key` 的底层图片能力

### 1.2 当前实现的根本问题

当前问题不在“飞书不会发卡片”，而在“谁来决定怎么展示”。

现状仍然是：

- 是否图表化，依赖用户提示词是否命中 `_looks_like_chart_request(...)`
- 是否发卡片，依赖模型是否显式输出 `feishu_skill_contract`
- 没有专门的“深度分析结果展示编排”步骤
- 没有“展示质量”校验
- 没有“生成失败后重试”的展示链路
- 没有完整支持图片块进入最终卡片

这会导致结果经常出现：

- 没有图
- 只有一张图
- 图有了但没有文字结论
- 多维分析只输出单块内容
- 图片可视化资产没有编进卡片

## 2. 目标

飞书渠道默认将 Agent 的深度分析结果整理成适合阅读的卡片输出：

- 文字结论保留
- 可结构化的数据尽量图表化
- 多结论支持多块展示
- 图片类可视化资产可进入卡片
- 不依赖用户额外说“图展示”
- 不允许通道层猜测补图
- 生成不合格时必须重试
- 重试耗尽后退回文字版，并附简要失败原因

## 3. 总体架构

采用三阶段架构：

### 3.1 主分析阶段

主 Agent 正常完成分析，输出主回答结果。

输出内容包括：

- 最终文本回答
- 中间结构化数据（如果有）
- artifacts / attachments（如果有）
- 其他 metadata

### 3.2 展示编排阶段

新增一个专门的“展示生成步骤”。

输入：

- 主分析结果
- 最终文本回答
- 可用结构化数据
- 可用图片/附件信息

输出：

- 高质量 `feishu_skill_contract`

这一阶段由模型负责，但任务范围严格限制为“展示编排”，不允许新增事实。

### 3.3 飞书渲染阶段

飞书通道仅负责：

- schema 校验
- quality 校验
- 校验失败时组织重试
- 合格后渲染卡片
- 最终失败时退回文字版

通道层不做自动识别图表，不做猜测，不做兜底补图。

## 4. 保留并扩展现有协议

### 4.1 不另起一套最终协议

最终渲染协议继续沿用现有：

- `metadata.feishu_skill_contract`

原因：

- 现有 `feishu.py` 已能渲染
- 现有 `feishu_contract.py` 已有 schema validator
- 已有测试基础
- 可减少重构成本

### 4.2 扩展协议能力

现有 `feishu_skill_contract.card_payload.blocks` 继续作为主结构，支持并规范以下 block：

- `markdown`
- `table`
- `chart`
- `image`

建议的最终 block 集合：

```json
{
  "feishu_skill_contract": {
    "card_schema_version": "v1",
    "target_channel": "feishu",
    "render_mode": "card",
    "fallback_text": "可视化结果生成失败，已返回文字版摘要。",
    "card_payload": {
      "title": "近14天花费分析",
      "summary": "整体花费呈上升趋势，5月3日出现峰值。",
      "blocks": [
        {
          "type": "markdown",
          "markdown": "### 核心结论\n近14天整体上升，峰值由渠道A带动。"
        },
        {
          "type": "chart",
          "chart": {
            "chart_type": "line",
            "title": "近14天花费趋势",
            "dimension": {
              "name": "日期",
              "values": ["04-23", "04-24", "04-25"]
            },
            "metrics": [
              {
                "name": "花费",
                "values": [120, 135, 128]
              }
            ],
            "why_this_chart": "展示时间趋势变化"
          }
        },
        {
          "type": "table",
          "table": {
            "title": "异常波动明细",
            "columns": ["日期", "花费", "原因"],
            "rows": [["05-03", "220", "渠道A投放增加"]]
          }
        },
        {
          "type": "image",
          "image": {
            "image_key": "img_xxx",
            "caption": "相关可视化附图"
          }
        }
      ]
    }
  }
}
```

## 5. 模型层可控方案

### 5.1 不让主分析顺手输出展示协议

不要依赖主分析 Agent 在同一次生成里顺手把飞书卡片也写好。

原因：

- 任务耦合太重
- 格式稳定性差
- 容易出现只有图、无文字结论
- 容易出现图块数量不足

### 5.2 单独新增展示编排步骤

在主分析完成后，增加一个独立模型步骤。

职责：

- 读取主分析结果
- 整理标题、摘要、关键发现
- 决定哪些内容用文字、图表、表格、图片
- 生成完整 `feishu_skill_contract`

限制：

- 不允许新增事实
- 不允许丢掉核心结论
- 不允许只输出图表
- 不允许自由发挥飞书底层 JSON 之外的结构

### 5.3 展示生成器的受控输出规则

展示生成器必须遵守：

- 必须输出 `title`
- 必须输出 `summary`
- 必须输出 `blocks`
- `blocks` 至少包含一个文字型 block
- 图表必须服务于某条结论
- 多结论分析应有多个展示块
- 若有有效图片资产，应考虑 image block
- 若无合适图表，不允许硬凑图；可用 table + markdown，但必须明确

## 6. 校验机制

分为两层。

### 6.1 Schema 校验

复用并扩展 `backend/app/channels/feishu_contract.py`。

现有已覆盖：

- version
- target_channel
- render_mode
- chart/table/markdown 基本结构
- 数值长度匹配

新增内容：

- `image block` 校验
- `chart.title` 必填
- `summary` 必填
- `title` 必填
- `blocks` 非空
- `why_this_chart` 字段校验

### 6.2 Quality 校验

新增一层展示质量规则，不属于 schema，而属于业务质量。

建议规则：

1. 必须有 `title`
2. 必须有 `summary`
3. `blocks` 至少 2 个
4. 至少有 1 个文字类 block
5. 禁止只有 chart block 没有文字结论
6. 深度分析场景下，必须有至少 1 个数据展示 block
7. 如果存在多个独立结论，不能只输出 1 个孤立图块
8. chart block 必须有 `title`
9. chart block 必须有 `why_this_chart`
10. 若有 image block，必须有 `caption` 或上下文说明
11. 文字和图表顺序必须可读，不能全部堆在末尾
12. 不允许空 summary + 单图这种半成品结构

## 7. 重试机制

### 7.1 触发时机

当展示生成器输出 `feishu_skill_contract` 后：

- 先做 schema 校验
- 再做 quality 校验

任一失败即触发重试。

### 7.2 重试输入

重试时不重新分析业务问题，只重新生成展示协议。

输入包括：

- 原始主分析结果
- 上次生成的 contract
- 校验失败原因列表

示例：

```json
{
  "retry_feedback": {
    "schema_errors": [],
    "quality_errors": [
      "缺少 summary",
      "只有图表块，缺少文字结论块",
      "多结论分析仅输出一个展示块"
    ]
  }
}
```

### 7.3 重试次数

建议：

- 最大重试次数：3 次

即：

- 首次生成
- 最多 2 次修正重试

### 7.4 重试失败后的策略

若重试耗尽仍不合格：

- 不发送不合格卡片
- 回退为文字版输出
- 附带简要失败说明

示例：

```text
近14天花费整体呈上升趋势，5月3日达到峰值，主要由渠道A带动。

可视化展示生成失败：
- 缺少合格的摘要与结论块
- 图表编排未满足展示质量要求

已改为发送文字版结果。
```

注意：

- 失败说明要用户可读
- 不暴露内部 prompt 或技术细节
- 不输出复杂堆栈

## 8. 与当前代码的对应改造点

### 8.1 `backend/app/channels/manager.py`

职责调整：

- 不再把“是否图表请求”作为核心触发条件
- 飞书 final 阶段统一触发展示编排步骤
- 接收展示生成结果
- 执行校验与重试
- 失败后组织文字版回退

建议处理方式：

- 弱化 `_looks_like_chart_request(...)` 的主导地位
- 将其从“是否走图表链路”改为“可作为辅助上下文”
- 主判断改为：`channel_name == "feishu"` 且 `is_final == True`

### 8.2 `backend/app/channels/feishu_contract.py`

职责扩展：

- 保留 schema validator
- 新增 `image block` 校验
- 新增 quality validator
- 输出结构化校验错误，供重试使用

建议新增接口：

- `validate_and_normalize_contract(...)`
- `validate_contract_quality(...)`

### 8.3 `backend/app/channels/feishu.py`

职责扩展：

- 保持现有 contract 渲染器角色
- 在 `_build_card_from_contract_payload(...)` 中新增 `image block`
- 保留 `markdown/table/chart`
- 不新增任何猜测逻辑

### 8.4 新增展示生成模块

建议新增模块，例如：

- `backend/app/channels/feishu_presentation_builder.py`

职责：

- 基于主分析结果生成 `feishu_skill_contract`
- 接受 retry feedback 进行重生成
- 保证输出只面向飞书展示编排

如果更希望放到 agent/harness 层，也可以放到：

- `backend/packages/harness/deerflow/agents/...`

但从当前调用路径看，先放到 channel orchestration 边界附近会更容易落地。

## 9. 图片能力接入方案

当前代码已有 `image_key` 基础能力，因此应正式支持 `image block`。

建议 block 结构：

```json
{
  "type": "image",
  "image": {
    "image_key": "img_xxx",
    "caption": "异常点可视化图"
  }
}
```

渲染要求：

- 若 `image_key` 存在，渲染成飞书图片元素
- 若 `caption` 存在，在图片前后增加 markdown 说明
- 不允许 image block 裸奔，没有说明

## 10. 交付阶段

### Phase 1：协议与校验

目标：

- 扩展 `feishu_skill_contract` 文档定义
- 增加 `image block`
- 增加 quality 校验器
- 定义 retry feedback 结构

产出：

- 协议字段清单
- quality rules
- 单元测试

### Phase 2：展示生成与重试

目标：

- 新增展示生成步骤
- 接入 `manager.py`
- 实现校验失败重试
- 实现最终失败退回文字版

产出：

- 生成链路
- 重试链路
- 失败回退链路

### Phase 3：飞书渲染扩展

目标：

- `feishu.py` 支持 `image block`
- 优化多块卡片编排顺序
- 打通图表 + 文本 + 图片的完整输出

产出：

- 最终卡片渲染能力
- 集成测试

## 11. 验收标准

满足以下条件视为完成：

1. 用户不写“图展示”，飞书也能触发展示编排
2. 深度分析结果默认优先输出卡片
3. 卡片中同时包含文字结论与图表/表格
4. 复杂分析支持多块展示，不再只是一张裸图
5. 图片可视化资产可进入卡片
6. 不合格展示协议会自动重试
7. 重试耗尽后退回文字版，并附简要失败原因
8. 通道层不做自动猜测图表
9. 现有 `feishu_skill_contract` 渲染链路继续可用

## 12. 推荐实施顺序

1. 先补协议文档与 quality rules
2. 再补 `feishu_contract.py` 校验能力
3. 再接展示生成步骤
4. 再接重试与失败回退
5. 最后补 `image block` 渲染与集成测试

## 13. 实施进度

### 13.1 已完成

1. 已扩展 `backend/app/channels/feishu_contract.py`
2. 已新增 `image` block 的 schema 支持
3. 已为 `chart` 增加强制字段：
   `title`
   `why_this_chart`
4. 已新增 `validate_contract_quality(...)` 质量校验入口
5. 已新增 `backend/app/channels/feishu_presentation_builder.py`
6. 已完成展示生成 prompt、retry feedback、contract 候选提取骨架
7. 已接入 `backend/app/channels/manager.py` 的飞书最终输出链路：
   展示生成
   schema 校验
   quality 校验
   重试
   失败后文字版回退
8. 已扩展 `backend/app/channels/feishu.py` 支持 `image block`
9. 已补充关键测试：
   `test_feishu_parser.py` 中的 contract/image/quality 测试
   `test_channels.py` 中的展示生成成功与失败回退测试
10. 已完成语法级编译检查，当前涉及文件可编译通过
11. 已补充 streaming final 场景测试：
   contract 缺失时触发展示生成
   成功后写回 `feishu_skill_contract`
12. 已修正兼容路径：
   保留显式 `feishu_card_payload` 的优先兼容能力
13. 已补重试次数断言：
   展示生成失败回退场景当前断言为 1 次主分析 + 3 次展示生成尝试
14. 已补 `retry_feedback` 内容测试
15. 已补 streaming 异常后文字回退测试

### 13.2 当前状态

当前主链路已经从“仅依赖用户图表提示词 + 模型显式 contract”转向：

- 飞书最终结果统一尝试生成 `feishu_skill_contract`
- 生成结果必须通过 schema + quality 校验
- 校验失败会内部重试
- 重试耗尽后退回文字版，并附失败原因

### 13.3 ????

1. ???????????????
2. ??????????????????????
3. ????????????????

### 13.4 ????

???????????????

1. ????????????? `feishu_skill_contract`
2. ?????? schema + quality ??
3. ?????????
4. ????????????????
5. ?? `feishu_skill_contract` ? `feishu_card_payload` ???????
6. `image block` ???????????

??????????????????????????????????
