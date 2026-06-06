# Copilot 指令

本文档用于约束 Copilot 或其他 AI 代码助手在本仓库中的默认行为。

## 1. 总原则

- 这是一个 Rust 项目，不是 Go 项目。
- 不要建议或生成 Go 代码。
- 不要恢复已经移除的 Go 目录、Go 配置或 Go 工作流。
- 优先在现有 Rust 结构内补齐功能，而不是新开平行实现。

## 2. 项目结构认知

请默认理解为：

- 根 crate：`Cargo.toml`
- 服务代码：`src/`
- 核心逻辑：`src/core/`
- HTTP / 任务 / 日志：`src/service/`
- CLI：`src/bin/`
- 集成测试：`tests/`
- 测试样本：`tests/files/`

不要再生成以下旧结构：

- `main.go`
- `api/`
- `config/`
- `updater/`
- `utils/`
- `service-v2/`
- `mod.rs` 风格入口模块

## 3. 依赖选择

新增代码时请遵守：

- JSON：使用 `sonic-rs`
- YAML：使用 `yaml_serde`
- 序列化模型：使用 `serde`
- HTTP：沿用 `axum`
- 异步运行时：沿用 `tokio`
- git：沿用 `git2`
- codec：沿用 `cridecoder`

不要重新引入：

- `serde_json`
- `serde_yaml`
- Go FFI / CGO 桥接
- 多余的 JSON/YAML 替代实现

## 4. 配置与环境变量

仓库内只保留两份配置文件：

- `haruki-asset-configs.yaml`
- `haruki-asset-configs.example.yaml`

如果需要写配置相关代码，请假定：

- 默认配置文件名是 `haruki-asset-configs.yaml`
- 示例文件名是 `haruki-asset-configs.example.yaml`
- 敏感项优先走 `${env:VAR_NAME}`

常见环境变量：

- `HARUKI_CONFIG_PATH`
- `HARUKI_CONFIG_URI`
- `HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH`
- `HARUKI_ASSET_STUDIO_FFI_WORKER_PATH`
- `HARUKI_SHARED_AES_KEY_HEX`
- `HARUKI_SHARED_AES_IV_HEX`
- `HARUKI_EN_AES_KEY_HEX`
- `HARUKI_EN_AES_IV_HEX`
- `RUST_LOG`

不要在示例代码或新文件里写入真实密钥、真实 token、真实路径凭据。

## 5. 测试样本规则

测试样本固定放在：

- `tests/files/0703.usm`
- `tests/files/se_0126_01.acb`
- `tests/files/unityasset_long`
- `tests/files/jacket_s_712`

如果需要引用样本，请从 `tests/files/` 读取，不要再假定样本在仓库根目录。

## 6. 接口与行为约定

当前接口是：

- `GET /healthz`
- `POST /v2/assets/update`
- `GET /v2/jobs/{id}`
- `POST /v2/jobs/{id}/cancel`

不要擅自恢复旧的 `/update_asset` 风格接口，除非有明确指示。

## 7. 代码生成偏好

- 优先写最小必要改动。
- 优先复用已有 helper，不要重复造轮子。
- 对热路径避免重复编译 regex、重复构建大对象或重复做阻塞 IO。
- 在 async 请求路径里尽量避免同步阻塞文件系统操作。
- 新测试优先写稳定的轮询/等待逻辑，不要依赖很脆弱的固定 sleep。

## 9. 提交前必须满足

如果 Copilot 给出”完成版”代码，默认应满足：

```bash
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

如果做不到，应明确指出是哪里还没过，而不是假设可用。

## 10. 文档更新要求

以下情况要同步更新文档：

- 改了配置文件名
- 改了环境变量名
- 改了 CLI 用法
- 改了 Docker / Compose 运行方式
- 改了测试样本路径

优先更新：

- `README.md`
- `.env.example`
- `CLAUDE.md`
- `AGENTS.md`
- 本文件

## Git commits

All commit subjects must follow:

```text
[Type] Short description starting with capital letter
```

Allowed types:

| Type      | Usage                                                 |
|-----------|-------------------------------------------------------|
| `[Feat]`  | New feature or capability                             |
| `[Fix]`   | Bug fix                                               |
| `[Chore]` | Maintenance, refactoring, dependency or build changes |
| `[Docs]`  | Documentation-only changes                            |

Rules:

- Description starts with a capital letter.
- Use imperative mood: `Add ...`, not `Added ...`.
- No trailing period.
- Keep the subject at or below roughly 70 characters.
- **Agent attribution uses the standard Git `Co-authored-by:` trailer in the commit body, not a free-form `Agent:` line.** This makes GitHub render the co-author avatar on the commit page. The trailer must be on its own line, separated from the subject by a blank line, in the form `Co-authored-by: <Display Name> <email>`. Suggested values per agent:
  - Claude (any 4.x): `Co-authored-by: Claude Opus 4.7 <noreply@anthropic.com>` (substitute the actual model, e.g. `Claude Sonnet 4.6`, `Claude Haiku 4.5`)
  - Codex: `Co-authored-by: Codex <noreply@openai.com>`
  - Copilot: `Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`

Examples from this repo's history:

```text
[Feat] Add configurable asset export types
[Fix] Nuverse parse issue
[Chore] Update dependencies
[Feat] Replace git2 with git CLI and add commit signing (#16)
```

## GitHub Actions workflows

Use the standardized workflow layout in `.github/workflows`:

- `ci.yml` runs on `main` pushes, pull requests targeting `main`, and manual dispatch.
- Rust CI order: `cargo fmt --all -- --check`, `cargo check --locked --all-targets`, `cargo clippy --locked --all-targets -- -D warnings`, then `cargo test --locked`.
- `release.yml` is the standard release build entrypoint. It runs on `v*` tags and manual dispatch, builds release artifacts, uploads them with `actions/upload-artifact`, and publishes GitHub Release assets on tag pushes.
- `docker.yml` is the standard Docker entrypoint. It runs on `main` pushes, `v*` tags, PRs that touch Docker/build inputs, and manual dispatch. PRs build only; non-PR runs push GHCR images with lowercase image names and Docker metadata tags.

Workflow maintenance rules:

- Keep workflow filenames and top-level names aligned: `CI`, `Release`, `Docker`, and optional package-specific names.
- Use `actions/checkout@v6`, `actions/setup-go@v6`, `actions/upload-artifact@v7`, `actions/download-artifact@v8`, `softprops/action-gh-release@v3`, and current Docker actions (`setup-buildx@v4`, `login@v4`, `metadata@v6`, `build-push@v7`).
- Keep `permissions` minimal: `contents: read` for CI/Docker build-only work, `contents: write` for release publishing, and `packages: write` only when pushing container images.
- Use workflow `concurrency` keyed by workflow name and ref, with release jobs using `release-${{ github.ref_name }}` and `cancel-in-progress: false`.
- Do not reintroduce legacy workflow names such as `rust-ci.yml`, `build.yml`, `release-build.yml`, `docker-build.yml`, or `docker-release.yml` unless a package-specific workflow already exists and is intentionally preserved.
