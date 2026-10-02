# How to Contribute

This document describes how to contribute code and documentation to the rekuiper project.

## Report Security Vulnerabilities

Do not open a public GitHub issue for a security vulnerability. Report security vulnerabilities privately as described in [SECURITY.md](../../SECURITY.md). For severe security incidents, use the emergency contact in that file.

## Report a Defect

- If the defect is a security vulnerability, refer to [SECURITY.md](../../SECURITY.md). Do not submit a public issue.
- Search existing issues on GitHub under [Issues](https://github.com/ankur-paan/rekuiper/issues) to verify that the defect is not already reported.
- If no open issue exists, [open a new issue](https://github.com/ankur-paan/rekuiper/issues/new). Include a clear title, a detailed description, and a reproducible test case.

## Code and Documentation Contributions

You can contribute code for new features or defect fixes.

### One-Time Setup

Project maintainers review proposed code changes through GitHub pull requests. Complete this setup before you contribute code:

1. **Fork** the repository to your personal GitHub account.
2. **Clone** your fork locally:
   ```shell
   git clone https://github.com/<Github_user>/ekuiper.git
   ```
3. Add the upstream repository as an additional Git remote:
   ```shell
   git remote add upstream https://github.com/ankur-paan/rekuiper.git
   ```

You can use any IDE or text editor. For Go development, refer to [Editors and IDEs for GO](https://github.com/golang/go/wiki/IDEsAndTextEditorPlugins).

### Create a Branch in Your Fork

Work on your contribution in a branch in your forked repository. Create a local branch based on the `master` branch:

```shell
git fetch upstream
git checkout -b <my-branch> upstream/master
```

### Package Import Specification

Consistent package import order maintains code quality. This project uses `gci` to verify package import order. Group imports in this order:

1. Standard library packages
2. Third-party external packages
3. Local project packages

Example:

```go
import (
    "fmt"

    "github.com/sirupsen/logrus"

    "github.com/lf-edge/ekuiper/contract/v2/api"
)
```

In the project root directory, run this command to reorder imports:

```shell
gci write --skip-generated -s standard -s default -s "prefix(github.com/lf-edge/ekuiper)" .
```

In GoLand, enable automatic import sorting under `Settings > Editor > Code Style > Go > Imports`.

### Code Conventions

- Format your code with `go fmt` before you commit. The GitHub Actions CI pipeline rejects code that is not formatted with `go fmt`.
- Run static analysis with `make lint` to verify code quality.
  - If `gofumpt` errors occur, run `gofumpt -w .` in the project root directory.
  - Refer to [golangci-lint](https://golangci-lint.run/) for lint rule documentation.
- Use camelCase for configuration keys in configuration files.

### Debug the Code

To debug code in GoLand:

1. Full application debugging: Verify all directories in the `build_prepare` section of the [Makefile](https://github.com/lf-edge/ekuiper/blob/master/Makefile) exist in the project root. Add breakpoints. Open `cmd/kuiperd/main.go` and start the debugger. Create a stream or rule that executes the target code path.
2. Unit test debugging: Write and debug a unit test. For example, debug `TestMapConvert_Funcs` in `pkg/cast/cast_test.go`.

#### Debug EdgeX Integration

To debug EdgeX source or sink code, run external services in Docker containers and run rekuiper locally.

##### Expose the Message Bus

EdgeX uses Redis as the default message bus. To expose the message bus, edit the `docker-compose.yml` file. Change the port mapping of the `database` service from `127.0.0.1:6379` to `0.0.0.0:6379`, then restart services:

```yaml
 database:
   container_name: edgex-redis
   environment:
     CLIENTS_CORE_COMMAND_HOST: edgex-core-command
     CLIENTS_CORE_DATA_HOST: edgex-core-data
     CLIENTS_CORE_METADATA_HOST: edgex-core-metadata
     CLIENTS_SUPPORT_NOTIFICATIONS_HOST: edgex-support-notifications
     CLIENTS_SUPPORT_SCHEDULER_HOST: edgex-support-scheduler
     DATABASES_PRIMARY_HOST: edgex-redis
     EDGEX_SECURITY_SECRET_STORE: "false"
     REGISTRY_HOST: edgex-core-consul
   hostname: edgex-redis
   image: redis:6.2-alpine
   networks:
     edgex-network: { }
   ports:
     - 0.0.0.0:6379:6379/tcp
   read_only: true
   restart: always
   security_opt:
     - no-new-privileges:true
   user: root:root
   volumes:
     - db-data:/data:z
```

##### Configure EdgeX Locally

Configure `etc/sources/edgex.yaml` based on the message bus type:

| Message Bus | Type | Protocol | Server | Port |
| :--- | :--- | :--- | :--- | :--- |
| Redis Server | redis | redis | 10.65.38.224 | 6379 |
| MQTT Broker | mqtt | tcp | 10.65.38.224 | 1883 |
| ZeroMQ | zero | tcp | 10.65.38.224 | 5566 |

Example Redis configuration:

```yaml
default:
  protocol: redis
  server: 10.65.38.224
  port: 6379
  topic: rules-events
  type: redis
  # Could be 'event' or 'request'.
  # If the message is from app service, the message type is an event;
  # Otherwise, if it is from the message bus directly, it should be a request
  messageType: event
```

##### Enable Console Logging and Set the REST API Port

In `etc/kuiper.yaml`, set `consoleLog` to `true` and set `restPort` to `59720`:

```yaml
basic:
  debug: false
  consoleLog: true
  fileLog: true
  rotateTime: 24
  maxAge: 72
  ip: 0.0.0.0
  port: 20498
  restIp: 0.0.0.0
  restPort: 59720
  authentication: false
  prometheus: false
  prometheusPort: 20499
  ignoreCase: true
```

##### Run rekuiper Locally

Start rekuiper using the debug procedure described above.

### Testing

The project uses GitHub Actions to run unit tests and Functional Verification Tests (FVT). Verify that all tests pass on your pull request.

- Write Go unit tests to validate new code.
- Pull requests trigger the [FVT test suite](https://github.com/lf-edge/ekuiper/blob/master/test/README.md). Verify that all tests pass.

### Licensing

All code contributions are licensed under the Apache License 2.0. Add the correct license header to every new file.

### Sign-Off Commits

You must sign off each commit to certify origin. Configure `user.name` and `user.email` in Git, then use `git commit -s`.

### Synchronize Your Branch

Rebase your branch on the latest upstream changes before you submit a pull request:

```shell
git fetch upstream
git rebase upstream/master
```

Push changes to your fork. If you rebased previously pushed commits, use force push:

```shell
git push origin -f
```

### Submit Pull Requests

Base your pull requests on the `master` branch.

Submit small, focused pull requests. Squash commits into a single commit where appropriate:

```shell
git rebase -i upstream/master
```

Ensure all commit messages follow the guidelines below. Push to your branch and create the pull request on GitHub.

### Commit Message Guidelines

Commit messages must have a header, an optional body, and an optional footer:

```text
<type>(<scope>): <subject>
<BLANK LINE>
<body>
<BLANK LINE>
<footer>
```

Rules:
- The header with `<type>` is mandatory.
- The maximum line length is 100 characters.
- Reference related issues in the footer (for example, `Closes: #123`).

Example:

```text
feat: add Fuji release compose files
```

```text
fix(script): correct run script to use the right ports

Previously device services used wrong port numbers. This commit fixes the port numbers to use the latest port numbers.

Closes: #123, #245, #992
```

#### Revert

If a commit reverts a previous change, prefix the header with `revert:`. In the body, write: `This reverts commit <hash>.`

#### Type

The type must be one of the following:

- **feat**: New user-facing feature
- **fix**: Defect fix for the user
- **docs**: Documentation changes
- **style**: Formatting and stylistic corrections without code logic changes
- **refactor**: Code restructuring without bug fixes or feature additions
- **chore**: Build tasks and dependency maintenance
- **perf**: Performance improvements
- **test**: Test suite additions or modifications
- **build**: Build system or dependency changes
- **ci**: Continuous integration configuration changes
- **revert**: Revert of a previous commit

#### Scope

No predefined scopes exist. Use a custom scope when helpful.

#### Subject

The subject must contain a succinct description:
- Use imperative present tense (for example, "change", not "changed" or "changes").
- Do not capitalize the first letter.
- Do not add a period at the end.

#### Body

Use imperative present tense. Describe the motivation for the change and compare with previous behavior.

#### Footer

Document breaking changes with prefix `BREAKING CHANGE:`. Reference closed GitHub issues with `Closes: #<issue>`.

## Community Promotion

You can also contribute by promoting the project in the community:

- Integrate rekuiper into your open-source projects.
- Organize workshops or meetups.
- Answer user questions in GitHub Issues, Slack, or mailing lists.
- Write tutorials.
- Mentor new contributors.

## Roles and Responsibilities

### Contributor

Contributors are community members who contribute to the project. Any person can become a contributor. Common contribution activities include:

- Reporting and fixing defects.
- Reviewing requirements and software capabilities.
- Writing documentation.

Start with the [Code and Documentation Contributions](#code-and-documentation-contributions) guide and join the community Slack channel.

### Committer

Committers have direct access to project repositories. To qualify as a committer:

- Contribute actively to the rekuiper project.
- Express interest to maintainers.
- Submit 6 or more substantial pull requests.
- Demonstrate technical understanding of the codebase and project goals.

An existing maintainer nominates eligible contributors. Committers review issues and pull requests.

### Maintainer

Maintainers plan and design the project architecture. To qualify as a maintainer, committers must:

- Expand adoption and ecosystem integrations.
- Collaborate in community meetings and discussions.
- Demonstrate mastery of rekuiper architecture and strategy.
- Lead major feature designs and implementations.

An existing maintainer nominates candidates on the [Maintainer List](https://github.com/lf-edge/ekuiper/blob/master/MAINTAINERS.md).

### Nomination Process

The following table describes how the nomination is approved:

| Nomination | Description | Approval | Binding Roles | Minimum Length (days) |
| :--- | :--- | :--- | :--- | :--- |
| New Committer | Proposed by a maintainer | [Lazy Consensus](https://communitymgt.fandom.com/wiki/Lazy_consensus) | Active maintainers | 7 |
| New Maintainer | Proposed by a maintainer | Supermajority (2/3) Approval | Active maintainers | 7 |

