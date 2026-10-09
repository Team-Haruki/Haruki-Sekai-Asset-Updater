# AGENTS 指南

本文档面向在本仓库内协作的开发者与自动化代理，描述当前项目结构、基本约束和交付要求。

## 1. 项目现状

- 本仓库已经整理为 Rust 主项目。
- 旧 Go 主程序已经移除，不要再按 Go 服务结构新增主程序代码。
- 资产解包只保留直接链接 `unity-rs-core` 的纯 Rust 路径。
- 当前仓库为 Cargo workspace：
  - `Cargo.toml`
  - `crates/sekai-asset-client/`
  - `crates/sekai-asset-pipeline/`
  - `src/`
  - `tests/`
- 本地启动主服务使用 `cargo run`（需要 `haruki-asset-configs.yaml` 和相关环境变量），
  容器方式使用 `docker compose up --build`。
- 对外 HTTP 接口目前使用 v2 路径：
  - `GET /healthz`
  - `POST /v2/assets/update`
  - `GET /v2/jobs`
  - `GET /v2/jobs/{id}`
  - `POST /v2/jobs/{id}/cancel`
- 请求流程：`POST /v2/assets/update` 由 handler 创建任务，`JobManager` 派发 tokio task，
  经 `build_execution_plan` 得到 `ExecutionPlan`，再由 `AssetExecutionContext` 执行
  下载/解密/导出/上传，任务状态通过 `GET /v2/jobs/{id}` 查询。

## 2. 目录约定

- `src/`
  Rust 主服务应用代码；负责 HTTP、任务状态、批量调度、发布和 Git 同步。
- `crates/sekai-asset-pipeline/`
  可复用的单 bundle 执行内核；负责 provider/manifest 数据结构、crypto、Unity 导出、
  CRI/媒体后处理、安全路径与确定性产物清单，不依赖主服务。
- `crates/sekai-asset-client/`
  轻量 provider HTTP 客户端；负责版本解析、Cookie、manifest 获取和有界原子
  bundle 下载，不包含批量调度、持久缓存、发布或任务状态。
- `src/core/`
  核心业务逻辑，例如配置、下载、导出、上传、git 同步。
- `src/service/`
  HTTP、任务管理、日志等服务层逻辑。
- `tests/`
  集成测试。
- `tests/files/`
  不提交大体积 codec 样本；如需运行真实样本 baseline，将 `0703.usm` 与
  `se_0126_01.acb` 放在外部目录，并设置 `HARUKI_CODEC_SAMPLE_DIR`。
- `docs/migration/`
  当前保留 `v2-api.md`，用于记录 HTTP v2 API 说明。

## 3. 配置文件约定

- 本地实际运行配置：
  - `haruki-asset-configs.yaml`
- 示例配置：
  - `haruki-asset-configs.example.yaml`
- 不要重新引入多份 `smoke`、`fullchain`、`export` 专用配置到仓库根目录，除非有明确长期维护价值。
- 配置中的敏感字段优先使用 `${env:VAR_NAME}` 引用，而不是直接写死。

当前常用环境变量包括：

- `HARUKI_CONFIG_PATH`
- `HARUKI_CONFIG_URI`（配合 `HARUKI_CONFIG_OPENDAL_SCHEME` / `HARUKI_CONFIG_OPENDAL_ROOT` / `HARUKI_CONFIG_OPENDAL_OPTION_*` 从远端加载配置）
- `HARUKI_ASSET_STUDIO_READ_BATCH_SIZE`
- `HARUKI_MEDIA_BACKEND`
- `HARUKI_SHARED_AES_KEY_HEX`
- `HARUKI_SHARED_AES_IV_HEX`
- `HARUKI_EN_AES_KEY_HEX`
- `HARUKI_EN_AES_IV_HEX`
- `RUST_LOG`

并发/CPU/内存调优类变量（`HARUKI_DOWNLOAD_CONCURRENCY`、`HARUKI_POST_PROCESS_CONCURRENCY`、
`HARUKI_AUDIO_ENCODE_CONCURRENCY`、`HARUKI_VIDEO_ENCODE_CONCURRENCY`、
`HARUKI_MAX_IN_FLIGHT_BUNDLE_BYTES`、`HARUKI_CPU_BUDGET_*` 等）见 README 的
"Runtime Tuning" 一节。

## 4. 依赖与实现约束

- JSON 处理统一使用 `sonic-rs`，不要新增 `serde_json` 依赖。
- YAML 处理统一使用 `yaml_serde`，不要新增 `serde_yaml` 依赖。
- codec 后端统一依赖 crates.io 上的 `cridecoder`。
- 图片转换优先保持纯 Rust 路径，不要重新引入外部 WebP 工具链。
- 资产引擎为 crates.io 发布的 `unity-rs-core` crate，由 `Cargo.lock` 锁定版本并直接编译进二进制；
  没有动态库，也不需要 .NET 工具链。
- `unity-rs-core` 使用公开 crates.io registry，不需要私有 Git 依赖构建凭据。
- 共享 pipeline crate 直接调用 `unity-rs-core` 的纯 Rust API，这是唯一的资产引擎运行路径。
- `unity-rs-core`、`cridecoder` 和可选的 `rsmpeg` 由
  `sekai-asset-pipeline` crate 统一持有；主服务通过该 crate 使用这些能力。
- 共享 crate 不得反向依赖 Axum、JobManager、下载记录、OpenDAL 发布、Haruki 3D
  或 Git 同步等应用层能力。
- FFmpeg 是仅存的外部运行依赖（使用 media FFI feature 时链接其系统库）。
- FFmpeg 必须是 7.x：`rsmpeg` 的绑定与版本强绑定，更高版本会在 rsmpeg 内部报类型错误，
  而不会给出任何与版本相关的提示。

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
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

`--all-features` 会打开 `media-ffi`，让 `rsmpeg` 链接本机 FFmpeg。不加该 feature 时，
`crates/sekai-asset-pipeline/src/media/ffi.rs`（仓库中最大的 unsafe 面）不会被任何本地
检查覆盖。macOS + Homebrew 下默认 `pkg-config` 会解析到当前 `ffmpeg`，需显式指向
`ffmpeg@7`：

```bash
export PKG_CONFIG_PATH=/opt/homebrew/opt/ffmpeg@7/lib/pkgconfig
export FFMPEG_PKG_CONFIG_PATH=/opt/homebrew/opt/ffmpeg@7/lib/pkgconfig
```

CI 在 Linux 构建，容器内已固定 FFmpeg 7。

Sonar/覆盖率相关变更还应运行：

```bash
cargo llvm-cov --locked --workspace --lcov --output-path lcov.info --fail-under-lines 90
```

Pull Request 的变更代码覆盖率同样不得低于 90%。CI 的 `Diff coverage` job 用 `diff-cover`
对覆盖率构件（`coverage/lcov-rust.info`）和目标分支的差异执行该门槛。

如果改动涉及以下内容，还需要额外关注：

- 配置解析：
  检查 `src/core/config.rs` 相关测试是否覆盖。
- 样本导出：
  确认 `tests/codec_smoke.rs` 通过。
- unity-rs 集成：
  确认主服务通过共享 pipeline crate 链接 `unity-rs-core`，且 release 只交付主服务二进制。
- HTTP/任务流：
  确认 `tests/api.rs` 通过。
- 日志：
  确认 `tests/logging.rs` 通过。
- AssetStudio 集成（CI 可选）：
  确认 `tests/assetstudio_real.rs` 通过。
- Docker/3D 导出器构建输入：
  确认 `tests/dockerfile_haruki_3d.rs` 通过。

## 7. 对代理的特殊要求

- 不要重新引入 Go 主程序、Go 服务结构或 Go 运行配置。
- 不要新增其他资产引擎运行时或跨语言资产解包绑定。
- 不要把一次性调试输出、手工 smoke 配置、临时导出目录提交进仓库。
- 不要在仓库里写入真实密钥、真实 token、真实云存储凭据。
- 如果修改 CI，请确保：
  - `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  - `cargo test --workspace`
  仍然能跑通。
- 版本号只写在 `Cargo.toml`（以及 `Cargo.lock` 中本包自己的条目）：先提 bump PR，合并后再打同名
  `v<version>` tag，release-gate 会拒绝不一致的 tag。CI 不改写 `Cargo.toml`、不重新生成 `Cargo.lock`。
- Release artifact 与 Docker 镜像只需包含主服务二进制；`unity-rs-core` 已直接编译进主服务。

## 8. 推荐工作流

1. 先阅读 `README.md` 和本文件。
2. 只在 Rust 结构内工作。
3. 修改后先跑 `cargo fmt`。
4. 再跑 `cargo clippy`。
5. 最后跑 `cargo test`。
6. 只在确认通过后再准备提交。

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

## Release notes

Release notes follow the org standard
[RELEASE_NOTES.md](https://github.com/seiunx-dev/ci-templates/blob/main/RELEASE_NOTES.md),
written in English.

- Title every release with the tag only, for example `v7.2.3`.
- Publish a release as a pre-release only when its tag has an `-alpha`, `-beta` or `-rc`
  suffix; every tag gets a release.
- Omit empty sections, and end every item with its PR number `(#123)` (the short commit
  SHA when there is no PR).
- After `Release` publishes the GitHub Release, rewrite its auto-generated notes to the
  standard (`gh release edit <tag> --notes-file <file>`).
