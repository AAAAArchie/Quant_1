# AGENTS.md

## 目的

本文件定义了在当前项目中运行的任何编码代理默认应遵守的工作规则。

目标如下：
- 让改动尽可能小且正确
- 除非用户明确要求，否则保持现有架构和风格
- 明确说明验证过程
- 避免在常规工程任务中产生不必要的来回沟通
- 在编写任何代码前，先描述你的方法并等待批准
- Plan模型默认产出验收清单 + 失败条件
- 如果我给出的需求模糊不清，请在编写代码前提出澄清问题。
- 编写完成任何代码后，列出边缘案例并建议覆盖它们的测试用例。
- 如果一项任务需要修改超过 3 个以上文件，先停止并将其拆分成更小的任务
- 出现 bug 时，先编写能重现该 bug 的测试，再修复直到测试通过
- 每次我纠正你时，反思你做错了什么，并将其写入AGENTS.md，永不再犯
- 回答关于过往工作或决策的问题前，先查阅记忆文件

## Reflection Notes

- When the user narrows the implementation scope, do not broaden it based on available APIs. Restate the exact scope and implement only that scope.

