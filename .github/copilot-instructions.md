# Copilot 指令

本文档用于约束 Copilot 或其他 AI 代码助手在本仓库中的默认行为。

## 1. 总原则

- 这是一个 Rust 项目，不是 Go 项目。
- 不要建议或生成 Go 代码。
- 不要恢复已经移除的 Go 目录、Go 配置或 Go 工作流。
- 优先在现有 Rust 结构内补齐功能，而不是新开平行实现。

## 2. 项目结构认知

请默认理解为：

- Cargo workspace：根 `Cargo.toml`
- 单 bundle 执行内核：`crates/sekai-asset-pipeline/`
- provider HTTP 客户端：`crates/sekai-asset-client/`
- 主服务应用：`src/`
- 应用核心逻辑：`src/core/`
- HTTP / 任务 / 日志：`src/service/`
- 集成测试：`tests/`

共享边界必须保持：

- `sekai-asset-pipeline` 持有 provider/manifest 数据结构、crypto、安全路径、
  `unity-rs-core`、`cridecoder`、可选 `rsmpeg` 和确定性产物清单。
- `sekai-asset-client` 持有版本、Cookie、manifest HTTP 和有界原子下载。
- Axum、JobManager、批量调度、下载记录、OpenDAL 发布、Haruki 3D 和 Git
  同步只属于主服务；共享 crate 不得反向依赖这些能力。

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
- git：沿用 Git CLI，不要重新引入 `git2`
- codec：沿用 `cridecoder`
- 资产引擎：沿用 crates.io 的 `unity-rs-core`，并仅由
  `sekai-asset-pipeline` 直接依赖
- 图片转换：沿用纯 Rust 路径

不要重新引入：

- `serde_json`
- `serde_yaml`
- Go FFI / CGO 桥接
- 其他资产引擎运行时或跨语言资产解包绑定
- 多余的 JSON/YAML 替代实现

## 4. 配置与环境变量

配置文件只有两个名字：

- `haruki-asset-configs.yaml`：本地实际运行配置，被 `.gitignore` 忽略，不得提交
- `haruki-asset-configs.example.yaml`：仓库内唯一提交的配置模板

如果需要写配置相关代码，请假定：

- 默认配置文件名是 `haruki-asset-configs.yaml`
- 示例文件名是 `haruki-asset-configs.example.yaml`
- 敏感项优先走 `${env:VAR_NAME}`

常见环境变量：

- `HARUKI_CONFIG_PATH`
- `HARUKI_CONFIG_URI`
- `HARUKI_ASSET_STUDIO_READ_BATCH_SIZE`
- `HARUKI_MEDIA_BACKEND`
- `HARUKI_SHARED_AES_KEY_HEX`
- `HARUKI_SHARED_AES_IV_HEX`
- `HARUKI_EN_AES_KEY_HEX`
- `HARUKI_EN_AES_IV_HEX`
- `RUST_LOG`

不要在示例代码或新文件里写入真实密钥、真实 token、真实路径凭据。

## 5. 测试样本规则

大体积 codec 样本不提交到仓库。真实样本 baseline 通过外部目录启用：

- 设置 `HARUKI_CODEC_SAMPLE_DIR=/path/to/codec-samples`
- 该目录可包含 `0703.usm` 和 `se_0126_01.acb`

不要把一次性 smoke 配置、临时导出目录或真实样本写入仓库。

## 6. 接口与行为约定

当前接口是：

- `GET /healthz`
- `POST /v2/assets/update`
- `GET /v2/jobs`
- `GET /v2/jobs/{id}`
- `POST /v2/jobs/{id}/cancel`

不要擅自恢复旧的 `/update_asset` 风格接口，除非有明确指示。

## 7. 代码生成偏好

- 优先写最小必要改动。
- 优先复用已有 helper，不要重复造轮子。
- 对热路径避免重复编译 regex、重复构建大对象或重复做阻塞 IO。
- 在 async 请求路径里尽量避免同步阻塞文件系统操作。
- 新测试优先写稳定的轮询/等待逻辑，不要依赖很脆弱的固定 sleep。

## 8. 提交前必须满足

如果 Copilot 给出”完成版”代码，默认应满足：

```bash
cargo fmt
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

如果做不到，应明确指出是哪里还没过，而不是假设可用。

Sonar/覆盖率相关变更还应生成 workspace LCOV，并同时保持整体与变更代码
行覆盖率不低于 90%：

```bash
cargo llvm-cov --locked --workspace --lcov --output-path lcov.info --fail-under-lines 90
```

## 9. 文档更新要求

以下情况要同步更新文档：

- 改了配置文件名
- 改了环境变量名
- 改了 CLI 用法
- 改了 Docker / Compose 运行方式
- 改了测试样本路径

优先更新：

- `README.md`
- `.env.example`
- `AGENTS.md`（`CLAUDE.md` 只是指向它的入口，不写内容）
- `crates/sekai-asset-client/README.md`
- `crates/sekai-asset-pipeline/README.md`
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
  - Claude: `Co-authored-by: Claude Fable 5 <noreply@anthropic.com>` (substitute the actual model, e.g. `Claude Opus 5`, `Claude Sonnet 5`, `Claude Haiku 4.5`)
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

CI reuses the shared templates in
[`seiunx-dev/ci-templates`](https://github.com/seiunx-dev/ci-templates) at `@v1`.
The files in `.github/workflows` are thin callers:

- `ci.yml` (`CI`) runs on `main` pushes, pull requests targeting `main`, and manual
  dispatch:
  - `Rust` (`rust-ci`): fmt, clippy `--workspace --all-targets -D warnings`, and the
    workspace tests run once under `cargo llvm-cov` with a 90% line-coverage floor.
  - `Rust (media-ffi)` (`rust-ci` in `debian:trixie-slim` with the FFmpeg 7.1 dev
    packages, same as the runtime image): clippy and tests with
    `--features haruki-sekai-asset-updater/media-ffi`.
  - `Diff coverage` (PRs only, custom job in the caller): `diff-cover` ≥ 90% on the
    changed lines against the base branch, fed by the `Rust` job's coverage artifact.
  - `Sonar` scans that coverage (skipped green on Dependabot/fork PRs); `Workflow lint`
    runs actionlint.
  - `Docker` does not wait for the tests and builds on every PR (no path filter; the
    old one missed `crates/**`). On `main` it pushes the immutable
    `ghcr.io/team-haruki/haruki-sekai-asset-updater:sha-<full sha>` and `:sha-<7 chars>`
    as soon as the build finishes; the `Docker tags` job (`docker-retag.yml`, after
    `CI OK`) then moves `:main` to that digest without rebuilding, so `:main` only
    follows commits whose `CI OK` passed. The Dockerfile uses cargo-chef; the registry
    `:buildcache` keeps the cooked dependency layer.
- The aggregate job **`CI OK`** is the single CI verdict: `Docker tags` and
  `release-gate` wait for it. `main` has no branch protection or rulesets, so GitHub
  does not enforce it as a required check; it gates merges by convention only.
- `release.yml` (`Release`): bump `version` in `Cargo.toml` (and the package's own entry
  in `Cargo.lock`) in a PR → merge and wait for `CI OK` on `main` → push the tag
  `v<version>`. `release-gate` refuses a tag that differs from `Cargo.toml` and waits
  for `CI OK` on the tagged commit; then the binaries are built from the committed
  `Cargo.lock` (tags only, without `media-ffi`;
  `haruki-sekai-asset-updater-{linux-x64,macos-arm64}.tar.gz` and `-windows-x64.zip`,
  each with a top-level `haruki-sekai-asset-updater-<label>/` folder), the `main` image
  `:sha-<sha>` is promoted (re-tagged, not rebuilt) to `:<version>`, `:<major>.<minor>`
  and `:latest`, and the GitHub Release is published with `SHA256SUMS-<tag>.txt`.
  Manual dispatch is a dry run: it builds the binaries and publishes nothing.
- CI never rewrites `Cargo.toml` or regenerates `Cargo.lock`. The Dockerfile's
  `HARUKI_PACKAGE_VERSION` build arg is only for manual builds; it rewrites this
  package's own version entries and keeps the locked dependency set.

Workflow maintenance rules:

- Use the shared templates first. Add custom jobs or steps only when a template
  genuinely cannot meet the project's needs, keep them in the thin caller files, and
  add a comment explaining why.
- Template bugs and missing features are fixed upstream in `seiunx-dev/ci-templates`
  (new `v1.x.y` tag), not worked around here.
- Keep top-level `permissions: contents: read`; grant `packages: write` / `contents: write`
  only on the job that needs it.
- Do not suppress `githubactions:S7637` (full-SHA pins) in `sonar-project.properties`: the
  template's `sonar.yml` already ignores it for the `@v1` references.
- Third-party actions in caller-side custom steps are pinned to a full commit SHA with a
  `# vX.Y.Z` comment; Dependabot (`github-actions`) updates them and the template refs.
