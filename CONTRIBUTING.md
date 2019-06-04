# Contributing Guidelines

eee-identity-matching project is proprietary licensed and accepts
contributions via GitHub pull requests.  This document outlines some of the
conventions on development workflow, commit message formatting, contact points,
and other resources to make it easier to get your contribution accepted.

## How to Contribute

Pull Requests (PRs) are the main and exclusive way to contribute to the official eee-identity-matching project.
In order for a PR to be accepted it needs to pass a list of requirements:

- All PRs must be written in idiomatic Go, formatted according to [goimports](https://godoc.org/golang.org/x/tools/cmd/goimports), and without any warnings from [go lint](https://github.com/golang/lint) nor [go vet](https://golang.org/cmd/vet/).
- New features should be generally covered with tests.
- The test suite must pass.
- All PRs have to pass the personal evaluation of at least one of the [maintainers](MAINTAINERS.md).

### Format of the commit message

The commit summary must start with a capital letter and with a verb in present tense. No dot in the end.

```
Add a feature
Remove unused code
Fix a bug
```

Every commit details should describe what was changed, under which context and, if applicable, the GitHub issue it relates to.
