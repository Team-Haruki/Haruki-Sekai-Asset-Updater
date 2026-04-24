# AGENTS 指南

本文档面向在本仓库内协作的开发者与自动化代理，描述当前项目结构、基本约束和交付要求。

## 1. 项目现状

- 本仓库已经整理为 Rust 主项目。
- 旧 Go 主程序已经移除，不要再按 Go 项目结构新增代码。
- 当前根 crate 为：
  - `Cargo.toml`
  - `src/`
  - `tests/`
- 对外 HTTP 接口目前使用 v2 路径：
  - `GET /healthz`
  - `POST /v2/assets/update`
  - `GET /v2/jobs/{id}`
  - `POST /v2/jobs/{id}/cancel`

## 2. 目录约定

- `src/`
  Rust 主服务代码。
- `src/bin/`
  辅助 CLI，例如 `usmexport`、`usmmeta`、`assetinfo_dump`、`s3ls`。
- `src/core/`
  核心业务逻辑，例如配置、下载、导出、上传、git 同步。
- `src/service/`
  HTTP、任务管理、日志等服务层逻辑。
- `tests/`
  集成测试。
- `tests/files/`
  测试样本文件，目前保留：
  - `0703.usm`
  - `se_0126_01.acb`
- `docs/migration/`
  迁移背景与兼容性说明。

## 3. 配置文件约定

- 本地实际运行配置：
  - `haruki-asset-configs.yaml`
- 示例配置：
  - `haruki-asset-configs.example.yaml`
- 不要重新引入多份 `smoke`、`fullchain`、`export` 专用配置到仓库根目录，除非有明确长期维护价值。
- 配置中的敏感字段优先使用 `${env:VAR_NAME}` 引用，而不是直接写死。

当前常用环境变量包括：

- `HARUKI_CONFIG_PATH`
- `HARUKI_ASSET_STUDIO_CLI_PATH`
- `HARUKI_SHARED_AES_KEY_HEX`
- `HARUKI_SHARED_AES_IV_HEX`
- `HARUKI_EN_AES_KEY_HEX`
- `HARUKI_EN_AES_IV_HEX`
- `RUST_LOG`

## 4. 依赖与实现约束

- JSON 处理统一使用 `sonic-rs`，不要新增 `serde_json` 依赖。
- YAML 处理统一使用 `yaml_serde`，不要新增 `serde_yaml` 依赖。
- codec 后端统一依赖 crates.io 上的 `cridecoder`。
- 图片转换优先保持纯 Rust 路径，不要重新引入外部 WebP 工具链。
- `AssetStudioModCLI` 与 `ffmpeg` 继续作为外部工具依赖。

## 5. 代码风格约定

- 优先延续现有模块划分，不要把核心逻辑重新打散。
- 入口模块使用扁平结构：
  - `src/core.rs`
  - `src/service.rs`
- 不要重新引入 `src/**/mod.rs` 风格，除非有强制性技术原因。
- 变更尽量保持现有命名风格与错误处理模式。
- 没有明确收益时，不要为了“抽象”而过度重构。

## 6. 测试与验收要求

提交前至少应运行：

```bash
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

如果改动涉及以下内容，还需要额外关注：

- 配置解析：
  检查 `src/core/config.rs` 相关测试是否覆盖。
- 样本导出：
  确认 `tests/codec_smoke.rs` 通过。
- CLI：
  确认 `tests/cli.rs` 通过。
- HTTP/任务流：
  确认 `tests/api.rs` 通过。
- 日志：
  确认 `tests/logging.rs` 通过。
- AssetStudio 集成（CI 可选）：
  确认 `tests/assetstudio_real.rs` 通过。

## 7. Git commit 规范

所有 commit **必须** 遵循以下格式：

```
[Type] Short description starting with capital letter
```

| Type      | 用途                           |
|-----------|-------------------------------|
| `[Feat]`  | 新功能或新能力                  |
| `[Fix]`   | Bug 修复                       |
| `[Chore]` | 维护、重构、依赖或构建变更       |
| `[Docs]`  | 仅文档变更                     |

规则：

- 描述以**大写字母**开头。
- 使用祈使语气（`Add ...` 而不是 `Added ...`）。
- 末尾不加句号。
- 标题行不超过 ~70 字符。
- 代理生成的 commit **必须** 在 commit body 中添加 `Co-Authored-By` 署名，标明自己的身份。

示例：

```
[Feat] Add batch save for download asset

Co-Authored-By: Claude <noreply@anthropic.com>
```

```
[Fix] Nuverse parse issue

Co-Authored-By: GitHub Copilot <noreply@github.com>
```

## 8. 对代理的特殊要求

- 不要重新引入 Go 代码、Go 工具链或 Go 配置。
- 不要把一次性调试输出、手工 smoke 配置、临时导出目录提交进仓库。
- 不要在仓库里写入真实密钥、真实 token、真实云存储凭据。
- 如果修改 CI，请确保：
  - `cargo clippy --all-targets --all-features -- -D warnings`
  - `cargo test`
  仍然能跑通。
- 如果修改 release / docker 流程，请保持 Git tag 版本号能正确传递到 Rust 构建物。

## 9. 推荐工作流

1. 先阅读 `README.md`、`CLAUDE.md` 和本文件。
2. 只在 Rust 结构内工作。
3. 修改后先跑 `cargo fmt`。
4. 再跑 `cargo clippy`。
5. 最后跑 `cargo test`。
6. 只在确认通过后再准备提交。
