# Contributing to Kaleidux

Thank you for your interest in contributing to Kaleidux. This project welcomes contributions from the community, whether you're fixing bugs, improving documentation, or proposing new features.

---

## Table of Contents

- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Project Structure](#project-structure)
- [How to Contribute](#how-to-contribute)
- [Branching Strategy](#branching-strategy)
- [Commit Standards](#commit-standards)
- [Pull Request Process](#pull-request-process)
- [Code Review](#code-review)
- [Testing](#testing)
- [Coding Standards](#coding-standards)
- [Kaleidux Type Shi (Philosophy)](#kaleidux-type-shi-philosophy)
- [Reporting Bugs](#reporting-bugs)
- [Suggesting Features](#suggesting-features)
- [Release Process](#release-process)
- [What Not To Do](#what-not-to-do)
- [Getting Help](#getting-help)

---

## Getting Started

Before contributing, please:

1. Read the [PROJECT_STANDARDS.md](./PROJECT_STANDARDS.md) for our Git workflow and conventions
2. Check existing [issues](https://github.com/Mjoyufull/Kaleidux/issues) and [pull requests](https://github.com/Mjoyufull/Kaleidux/pulls) to avoid duplicating work
3. Understand that **all code changes go through pull requests** — no exceptions
4. **Fork the repository** if you don't have write access (most contributors)

### Key Resources

- **Issue Tracker**: [GitHub Issues](https://github.com/Mjoyufull/Kaleidux/issues)
- **Discussions**: [GitHub Discussions](https://github.com/Mjoyufull/Kaleidux/discussions)
- **Project Standards**: [PROJECT_STANDARDS.md](./PROJECT_STANDARDS.md)
- **Usage Documentation**: [USAGE.md](./USAGE.md)
- **Project README**: [README.md](./README.md)

---

## Development Setup

### Prerequisites

Kaleidux is written in Rust. You will need:

- **Rust 1.89+ stable** (NOT nightly)
  ```sh
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  rustc --version  # Verify stable, not nightly
  ```
- **Cargo** (comes with Rust)
- **Git**

### Optional Dependencies

For full functionality during development:

- **GStreamer Plugins** - `gst-plugins-good`, `gst-plugins-bad`, `gst-libav` for video support.
- **Vulkan/GPU Drivers** - For WGPU hardware acceleration.
- **wayland-scanner** - For generating Wayland protocol bindings.
- **libX11-devel** - For X11 backend support.

### Fork and Clone

**For external contributors (most people):**

1. **Fork the repository** on GitHub:
   - Go to https://github.com/Mjoyufull/Kaleidux
   - Click the "Fork" button in the top-right corner
   - This creates your own copy of the repository

2. **Clone your fork**:

   ```sh
   # Replace YOUR_USERNAME with your GitHub username
   git clone https://github.com/YOUR_USERNAME/Kaleidux.git
   cd Kaleidux

   # Add the upstream repository as a remote
   # This lets you sync with the main repository before creating PRs
   git remote add upstream https://github.com/Mjoyufull/Kaleidux.git
   ```

   **Why add upstream?** The upstream remote lets you:
   - Fetch the latest changes from the main repository
   - Rebase your feature branch on top of the latest `dev` branch
   - Keep your fork synchronized with the main project

3. **Keep your fork up to date** (do this periodically, especially before starting new work):

   ```sh
   # Fetch latest changes from the main repository
   git fetch upstream

   # Update your fork's dev branch
   git checkout dev
   git merge upstream/dev
   git push origin dev
   ```

   **When to sync**: Before starting a new feature branch, or if you notice the main repository has new commits you want to include.

**For maintainers with write access:**

```sh
# Clone the repository directly
git clone https://github.com/Mjoyufull/Kaleidux.git
cd Kaleidux
```

### Build

```sh
# Build in debug mode (faster compilation)
cargo build

# Build in release mode (optimized)
cargo build --release

# Run directly
cargo run

# Run with arguments
cargo run -- --help
```

### Development Build

For faster iteration during development:

```sh
# Build and run in debug mode
cargo run --package kaleidux-daemon

# Watch for changes and rebuild automatically (requires cargo-watch)
cargo install cargo-watch
cargo watch -x "run --package kaleidux-daemon"
```

---

## Project Structure

```
kaleidux-daemon/src/
├── main.rs             # Daemon entry point & IPC loop
├── renderer.rs         # WGPU rendering pipeline & transitions
├── orchestration.rs    # Config management & monitor behaviors
├── monitor_manager.rs  # Wayland/X11 output management
├── queue.rs            # Wallpaper queue & file selection
├── video.rs            # GStreamer video playback logic
├── shaders.rs          # GLSL shader loading & parameter mapping
├── scripting.rs        # Rhai automation engine
├── wayland.rs          # Wayland-specific backend (Layer-Shell)
└── x11.rs              # X11-specific backend

kldctl/src/
└── main.rs             # CLI utility entry point

kaleidux-common/src/
└── lib.rs              # Shared types & IPC protocol definitions
```

### Module Responsibilities

- **kaleidux-daemon/src/main.rs**: Daemon entry point & IPC loop.
- **kaleidux-daemon/src/renderer.rs**: WGPU rendering pipeline & transition logic.
- **kaleidux-daemon/src/orchestration.rs**: Configuration management and monitor coordination.
- **kaleidux-daemon/src/monitor_manager.rs**: Wayland/X11 output management.
- **kaleidux-daemon/src/queue.rs**: Content selection and playlist management.
- **kaleidux-daemon/src/video.rs**: GStreamer integration for video playback.
- **kaleidux-daemon/src/shaders.rs**: Shader compilation and parameter mapping.
- **kldctl/src/main.rs**: Command-line utility for interacting with the daemon.
- **kaleidux-common/src/lib.rs**: Shared protocol definitions and common types.

---

## How to Contribute

There are many ways to contribute to Kaleidux:

### Code Contributions

- Fix bugs listed in [issues](https://github.com/Mjoyufull/Kaleidux/issues)
- Implement new features
- Improve performance
- Refactor code for clarity or maintainability

### Non-Code Contributions

- Improve documentation (see [Documentation Changes](#documentation-changes) below)
- Create example configurations
- Answer questions in [discussions](https://github.com/Mjoyufull/Kaleidux/discussions)
- Test new releases and report issues
- Package Kaleidux for other distributions
- Write tutorials or blog posts

### Documentation Changes

**Simple documentation fixes** (typos, grammar, formatting) can be pushed directly to `main` without going through the PR process:

**Criteria for direct push:**

- Changes only to `.md` files (README, USAGE, CONTRIBUTING, etc.)
- No code changes whatsoever
- Typo fixes, grammar corrections, formatting improvements

**Process:**

```bash
git checkout main
git pull origin main
# Make documentation changes
git commit -m "docs: fix typo in README"
git push origin main
# Sync to dev
git checkout dev
git merge main
git push origin dev
```

**For substantial documentation changes** (rewrites, new sections, structural changes), please use the normal PR process for review.

---

## Branching Strategy

**IMPORTANT**: Never push directly to `main` or `dev`. All changes go through pull requests.

### Primary Branches

| Branch   | Purpose                                                              | Push Policy                                                                        |
| -------- | -------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| **main** | Stable, production-ready code. Every commit is a tagged release.     | Never push directly. Merge only from release branches after testing and tagging.   |
| **dev**  | Integration branch. All features merge here before release branches. | Never push directly. Only receives merges from feature branches via pull requests. |

### Feature Branches

All work occurs in feature branches created from `dev`:

| Type     | Naming          | Purpose                                      |
| -------- | --------------- | -------------------------------------------- |
| Feature  | `feat/name`     | New features or functionality                |
| Fix      | `fix/name`      | Bug fixes                                    |
| Refactor | `refactor/name` | Code restructuring without changing behavior |
| Docs     | `docs/name`     | Documentation changes                        |
| Chore    | `chore/name`    | Tooling, dependencies, build updates         |

### Release Branches

| Type    | Naming            | Purpose                                               |
| ------- | ----------------- | ----------------------------------------------------- |
| Release | `release/version` | Prepare releases with version bumps and final testing |

Release branches are created from `dev` when a maintainer decides to release. They freeze a stable point in `dev` for release preparation, allowing ongoing PRs to continue merging into `dev` without affecting the release. See the [Release Process](#release-process) section for details.

**Note:** Releases do not go directly from `dev` to `main` (except for documentation-only changes as specified in PROJECT_STANDARDS.md).

### Standard Workflow

**For external contributors (using forks):**

```sh
# 1. Update your fork's dev branch
git fetch upstream
git checkout dev
git merge upstream/dev
git push origin dev

# 2. Create feature branch from dev
git checkout dev
git checkout -b feat/your-feature-name

# 3. Develop locally (commit freely)
git commit -am "wip: working on feature"

# 4. Prepare for PR (sync with latest dev and clean up commits)
# First, get the latest changes from the main repository
git fetch upstream

# Rebase your feature branch on top of the latest dev
# This doesn't lose your changes - it just moves your commits to be based on the latest code
git rebase upstream/dev

# Interactive rebase to clean up commit history (optional but recommended)
# This lets you squash, reword, or reorder commits before the PR
git rebase -i upstream/dev

# 5. Push feature branch to your fork
git push origin feat/your-feature-name

# 6. Open pull request on GitHub targeting Mjoyufull/Kaleidux:dev
# IMPORTANT: Enable "Allow edits by maintainers" checkbox
```

**For maintainers (direct access):**

```sh
# 1. Create feature branch from dev
git checkout dev
git pull origin dev
git checkout -b feat/your-feature-name

# 2. Develop locally (commit freely)
git commit -am "wip: working on feature"

# 3. Prepare for PR (clean up commits)
git fetch origin
git rebase origin/dev
git rebase -i origin/dev  # Interactive rebase to clean history

# 4. Push feature branch
git push origin feat/your-feature-name

# 5. Open pull request targeting dev
```

---

## Commit Standards

Follow **Conventional Commits** format:

```
type(optional-scope): short description

[optional body]

[optional footer]
```

### Commit Types

| Type       | Meaning                 |
| ---------- | ----------------------- |
| `feat`     | New feature             |
| `fix`      | Bug fix                 |
| `docs`     | Documentation only      |
| `refactor` | Code restructuring      |
| `perf`     | Performance improvement |
| `chore`    | Build, deps, tooling    |
| `test`     | Testing only            |
| `style`    | Whitespace, formatting  |
| `revert`   | Undo a commit           |

### Examples

```sh
feat(detach): implement --detach flag with systemd-run support
fix(db): enforce foreign key constraints properly
refactor(cache): move batch operations to separate module
docs(usage): add examples for dmenu mode
chore: update flake.nix to use naersk
```

### During Development

- Commit as you work — don't obsess over perfection
- "wip" and "temp fix" are valid local commits
- Clean up commit history before opening PR using `git rebase -i`

---

## Pull Request Process

### Before Submitting

1. **Rebase on latest dev**:

   ```sh
   # For external contributors using forks:
   git fetch upstream
   git rebase upstream/dev

   # For maintainers with direct access:
   git fetch origin
   git rebase origin/dev
   ```

   **Note**: Rebase doesn't delete your changes! It:
   - Takes your commits and replays them on top of the latest `dev` branch
   - Ensures your PR is based on the most recent code
   - Helps avoid merge conflicts when your PR is reviewed

2. **Run all checks**:

   ```sh
   cargo fmt
   cargo clippy -- -D warnings
   cargo test
   cargo build --release
   ```

3. **Clean commit history**:

   ```sh
   git rebase -i origin/dev
   ```

4. **Push branch**:
   ```sh
   git push origin feat/your-feature-name
   ```

### Opening a PR

1. **For external contributors**: Go to your fork on GitHub and click "New Pull Request"
   - **Base repository**: `Mjoyufull/Kaleidux`
   - **Base**: `dev` (NOT `main`)
   - **Compare**: `YOUR_USERNAME/Kaleidux:feat/your-feature-name`

   **For maintainers**: Go to the main repository and click "New Pull Request"
   - **Base**: `dev` (NOT `main`)
   - **Compare**: your feature branch

2. **IMPORTANT**: Enable the **"Allow edits by maintainers"** checkbox
   - This allows maintainers to make small fixes, rebase, or help resolve conflicts
   - This follows the collaborative philosophy in [PROJECT_STANDARDS.md](./PROJECT_STANDARDS.md)
   - Maintainers will respect your work and credit you appropriately

3. Use the PR template below

### PR Template

**Title**: `feat: add your feature` (follow conventional commits)

**Body**:

```markdown
## Summary

Brief description of what this PR does and why.

- [ ] I did basic linting
- [ ] I'm a clown who can't code 🤡

## Changes

- Added tag filtering UI
- Implemented tag persistence in database
- Updated documentation

## Testing

1. Build with cargo build --release
2. Run kaleidux-daemon and verify wallpapers render
3. Test IPC commands with kldctl

## Breaking Changes

None

## Related Issues

Closes #42
```

### Draft Pull Requests

GitHub allows you to open PRs as "drafts" - these are PRs that aren't ready for review yet but you want to show your progress.

**When to use draft PRs:**

- You want early feedback on approach before completing the work
- You're working on a large feature and want visibility into your progress
- You want architectural review before full implementation
- You're stuck and need help to continue

**How to create a draft PR:**

1. When opening a PR on GitHub, click the dropdown on "Create pull request"
2. Select "Create draft pull request" instead
3. The PR will be marked as draft and reviewers won't be notified
4. When ready, click "Ready for review" to convert it to a normal PR

**Note:** Draft PRs still target the `dev` branch and follow all other PR guidelines.

### PR Guidelines

- Target the `dev` branch, not `main`
- Use a clear, descriptive title following conventional commits format
- Keep PRs focused on a single feature or fix
- Break large changes into smaller, reviewable PRs
- Respond to review feedback promptly
- Be open to suggestions and constructive criticism

---

## Code Review

### What to Expect

- **Initial response**: A few hours to a few days
- **Full review**: Within 1 week
- **Merge after approval**: Within 1-2 days
- Reviewers may request changes or ask questions
- Multiple rounds of review may be necessary

### Internal Merging

Sometimes PRs are accepted but merged internally as part of larger refactoring efforts:

- You will be credited in commit messages and release notes
- The functionality you contributed will be included
- This is not a rejection, but integration into ongoing development

Example maintainer response:

> "Thank you for this contribution. Your approach is better than the current implementation. I'm currently refactoring the project structure, so I'll be merging this internally as part of that effort. You'll be credited in the commit message and release notes when it ships."

### Stale PRs

- PRs without activity for **30 days** will be marked stale
- Stale PRs will be closed after **14 additional days** of inactivity
- Exception: PRs marked as work-in-progress or on-hold by maintainers
- Closed PRs can be reopened if work resumes

### Review Criteria

| Aspect        | Expectation                   |
| ------------- | ----------------------------- |
| Correctness   | The code does what it claims  |
| Clarity       | Another dev can understand it |
| Impact        | Doesn't introduce regressions |
| Security      | No obvious vulnerabilities    |
| Style         | Matches existing conventions  |
| Documentation | Updated if needed             |

### Feedback Etiquette

- Comment with **why**, not just "change this"
- Nitpicks = non-blocking
- If it's broken, mark **Request Changes**
- Prefer questions over commands:
  > "Could this be simplified?" not "Simplify this."

---

## Testing

### Running Tests

```sh
# Run all tests
cargo test

# Run specific test
cargo test test_name

# Run tests with output
cargo test -- --nocapture

# Run tests with backtrace
RUST_BACKTRACE=1 cargo test
```

### Writing Tests

Add unit tests in the same file as the code:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_desktop_entry() {
        let entry = parse_entry("test.desktop");
        assert!(entry.is_ok());
    }
}
```

Add integration tests in `tests/` directory for end-to-end testing.

### Manual Testing

Before submitting a pull request, manually test:

1. Basic image wallpaper rendering.
2. Video wallpaper playback and looping.
3. Transitions between different content types.
4. Multi-monitor behaviors (independent/synchronized/grouped).
5. IPC commands via `kldctl` (next, prev, status, etc.).
6. Configuration reloading.

---

## Coding Standards

### Rust Style

- Follow the official [Rust Style Guide](https://doc.rust-lang.org/nightly/style-guide/)
- Use `rustfmt` for formatting:
  ```sh
  cargo fmt
  ```
- Use `clippy` for linting:
  ```sh
  cargo clippy -- -D warnings
  ```
- All code must compile without warnings on Rust stable

### Code Quality

- Write clear, self-documenting code
- Add doc comments for public APIs:
  ```rust
  /// Applies a new configuration to the renderer.
  ///
  /// # Arguments
  ///
  /// * `config` - The transition configuration to apply
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` on success, or an error if the configuration is invalid
  pub fn apply_config(&mut self, config: &TransitionConfig) -> Result<()> {
      // implementation
  }
  ```
- Keep functions focused and single-purpose
- Avoid deeply nested code
- Use meaningful variable and function names

### Error Handling

- Use `Result` and `?` operator for error propagation
- Provide context with error messages:
  ```rust
  .with_context(|| format!("Failed to read config file: {}", path.display()))?
  ```
- Handle errors gracefully and provide user-friendly messages

### Performance Considerations

- Avoid unnecessary allocations
- Use references where possible
- Profile before optimizing
- Document performance-critical sections

---

## Kaleidux Type Shi (Philosophy)

Contributing to Kaleidux means more than just writing code; it's about embracing a specific aesthetic and technical ethos.

1. **Performance is a feature**: We use `WGPU` and avoid unnecessary CPU work because smooth transitions shouldn't cost frames.
2. **Aesthetic Excellence**: Wallpapers are visual. Every transition, every blit, and every UI output should look "premium".
3. **Productive Chaos**: We value flow states. Burst commits are fine locally, but we polish for the history.
4. **Kaleidux type shi**: If it doesn't feel like it belongs in a high-end, cyberpunk-adjacent desktop setup, it might need more iteration.

---

## Reporting Bugs

If you find a bug, please [open an issue](https://github.com/Mjoyufull/Kaleidux/issues/new) with the following information:

### Bug Report Template

````markdown
**Description**
A clear and concise description of the bug.

**To Reproduce**
Steps to reproduce the behavior:

1. Run command '...'
2. Type '...'
3. Press '...'
4. See error

**Expected Behavior**
What you expected to happen.

**Actual Behavior**
What actually happened.

**Environment**

- Kaleidux version: [e.g., 0.0.1-kneecap]
- OS: [e.g., Arch Linux, kernel 6.6.1]
- Terminal: [e.g., kitty 0.30.0]
- GPU: [e.g., NVIDIA RTX 3060, Driver 545.29]
- Rust version: [output of `rustc --version`]
- Desktop Environment: [e.g., Sway, Hyprland]

**Configuration**
If relevant, include your config file or specific settings:

```toml
# Your config.toml contents
```
````

**Logs/Output**
If applicable, include error messages or logs:

```
Error output here
```

**Additional Context**
Any other information that might be relevant.

````

### Good First Issues

Look for issues labeled:
- `good first issue` - Simple bugs suitable for newcomers
- `bug` - Confirmed bugs that need fixing
- `help wanted` - Issues where maintainers need assistance

---

## Suggesting Features

Feature suggestions are welcome. Before suggesting a feature:

1. Check if it has already been suggested in [issues](https://github.com/Mjoyufull/Kaleidux/issues)
2. Consider if it fits Kaleidux's scope as a wallpaper daemon
3. Think about how it would be implemented

### Feature Request Template

```markdown
**Feature Description**
A clear description of the feature you'd like to see.

**Use Case**
Explain the problem this feature would solve or the workflow it would improve.

**Proposed Solution**
If you have ideas on how to implement this, describe them here.

**Alternatives Considered**
Other ways you've considered solving this problem.

**Additional Context**
Any other context, mockups, or examples.
````

---

## Release Process

**Note**: Only maintainers handle releases and all version updates. Contributors do not need to update version numbers.

### When to Create a Release Branch

A maintainer creates a release branch when:

- They decide it's time for a release
- `dev` is in a stable state (no critical bugs, features are complete)
- All planned features for the release are merged into `dev`

**Important:** Release branches freeze a specific point in `dev`, allowing ongoing PRs to continue merging into `dev` without affecting the release preparation. Releases do not go directly from `dev` to `main` (except for documentation-only changes as specified in PROJECT_STANDARDS.md).

### Preparation (Maintainers Only)

1. Ensure all feature PRs for the release are merged into `dev`
2. Confirm all tests pass on `dev`:
   ```sh
   cargo test
   cargo build --release
   ```
3. Create a release branch from `dev` (this freezes the release point):
   ```sh
   git checkout dev
   git pull origin dev
   git checkout -b release/v0.0.1-kneecap  # Replace with actual version
   ```
4. Update version references on the release branch:
   - `Cargo.toml` (root directory)
   - `flake.nix` (root directory)
   - `README.md` (installation instructions, if needed)
   - Man pages (`man/kaleidux-daemon.1`, `man/kldctl.1`)
5. Commit version bump:
   ```sh
   git commit -am "chore: bump version to 0.0.1-kneecap"
   ```
6. Prepare release notes following [Keep a Changelog](https://keepachangelog.com/)
7. Verify [Semantic Versioning 2.0.0](https://semver.org/) compliance
8. Run final tests on the release branch:
   ```sh
   cargo test
   cargo build --release
   ```

### Codename Policy

**Codenames change only on MAJOR version bumps:**

- Codename for 0.x.x series: `kneecap`
- Previous versions: [None]
- When 1.0.0 is released, a new codename will be chosen
- **Only maintainers** choose and assign codenames

All 0.x.x releases use `kneecap`.

### Process

```sh
# 1. Merge release branch to main
git checkout main
git pull origin main
git merge release/v0.0.1-kneecap

# 2. Tag the release
git tag -a v0.0.1-kneecap -m "v0.0.1-kneecap: initial release"
git push origin main --tags

# 3. Merge release branch back to dev (so dev has the version bump)
git checkout dev
git merge release/v0.0.1-kneecap
git push origin dev

# 4. Delete the release branch
git branch -d release/v0.0.1-kneecap
git push origin --delete release/v0.0.1-kneecap
```

**Why this workflow:**

- `dev` continues accepting PRs during release preparation
- Release work is isolated on the release branch
- No conflicts from ongoing development
- Clear freeze point for the release
- `dev` stays in sync with version numbers

### GitHub Release

Create a release using [Keep a Changelog](https://keepachangelog.com/) format:

```markdown
## [0.0.1-kneecap] - 2026-01-18

### Added

- Hardware-accelerated GLSL transitions using WGPU.
- Smooth video wallpaper playback via GStreamer integration.
- Multi-monitor support with independent rendering pipelines.
- Rhai scripting engine for wallpaper automation.

### Changed

- Transition system now supports over 50+ built-in shaders.
- Optimized texture uploads for low latency.

### Fixed

- Frame synchronization issues on Wayland.
- Resource leaks during rapid wallpaper switching.

### Notes

Initial alpha release focusing on core rendering stability.
```

---

## What Not To Do

### Absolutely Forbidden

- Push directly to `main` or `dev`
- Merge without PR
- Release without testing
- Ignore version updates in relevant files
- Skip running `cargo fmt` and `cargo clippy` before pushing

### Strongly Discouraged

- Inconsistent versioning
- Unreviewed breaking changes
- Merging with failing tests
- Ignoring clippy warnings
- Leaving PRs without response for weeks

---

## Getting Help

### Communication Channels

- **GitHub Issues**: For bugs and feature requests
- **GitHub Discussions**: For questions and general discussion
- **Pull Request Comments**: For questions about specific changes

### Questions About Contributing

If you're unsure about:

- How to implement a feature
- Whether a change would be accepted
- How to test something
- How to structure your code
- Anything else related to contributing

Please open a [discussion](https://github.com/Mjoyufull/Kaleidux/discussions) or comment on a related issue. The maintainers are happy to help guide you.

### Common Questions

**Q: I found a typo in the documentation. Do I still need to open a PR?**  
A: Yes, but it's a very quick process. Create a `docs/fix-typo` branch, make the change, push, and open a PR. Documentation improvements are always welcome.

**Q: My PR hasn't been reviewed yet. Should I ping someone?**  
A: Wait 2-4 days for initial response. If no response after 5 days, feel free to leave a polite comment on the PR.

**Q: Can I work on multiple features at once?**  
A: Yes, but create separate branches and PRs for each feature. This makes review easier and allows features to be merged independently.

**Q: I want to refactor a large part of the codebase. Should I do it?**  
A: Open an issue first to discuss the refactoring plan. Large refactors need coordination to avoid conflicts with ongoing work.

**Q: The maintainer wants to merge my PR internally. Did I do something wrong?**  
A: No! This means your contribution is good, but it needs integration with ongoing refactoring work or feature changes. You'll be credited in the release notes.

---

## Recognition

### Contributors

All contributors are recognized in:

- Release notes when their changes are included
- Git commit history with proper attribution
- GitHub contributor statistics
- Special thanks in major release announcements

### Types of Recognition

- **Code Contributors**: Listed in release notes for features and fixes
- **Documentation Contributors**: Credited in commit messages and release notes
- **Bug Reporters**: Thanked in issue closure and release notes
- **Feature Requesters**: Credited when features are implemented
- **Reviewers**: Acknowledged for helpful feedback

### Thank You

Every contribution, no matter how small, helps make Kaleidux better. Whether you're:

- Fixing a typo in documentation
- Reporting a bug
- Implementing a major feature
- Answering questions in discussions
- Testing release candidates

Your time and effort are genuinely appreciated. Thank you for contributing to Kaleidux.

---

## License

By contributing to Kaleidux, you agree that your contributions will be licensed under the GNU Affero General Public License Version 3 (AGPL-3.0), the same license as the project.

See the [LICENSE](./LICENSE) file for full details.

---

## Additional Resources

### Learning Resources

- [Rust Book](https://doc.rust-lang.org/book/) - Official Rust programming guide
- [Rust by Example](https://doc.rust-lang.org/rust-by-example/) - Learn Rust through examples
- [Clippy Lints](https://rust-lang.github.io/rust-clippy/master/) - Understanding clippy warnings
- [Conventional Commits](https://www.conventionalcommits.org/) - Commit message format
- [Keep a Changelog](https://keepachangelog.com/) - Changelog format

### Project-Specific Documentation

- [PROJECT_STANDARDS.md](./PROJECT_STANDARDS.md) - Git workflow and standards
- [USAGE.md](./USAGE.md) - User documentation
- [README.md](./README.md) - Project overview

---

**Questions?** If anything in this guide is unclear or you have suggestions for improving it, please open an [issue](https://github.com/Mjoyufull/Kaleidux/issues) or [discussion](https://github.com/Mjoyufull/Kaleidux/discussions).

Thank you again for contributing to Kaleidux!
