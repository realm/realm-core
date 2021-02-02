# Contributing

## Filing Issues

Whether you find a bug, typo or an API call that could be clarified, please [file an issue](https://github.com/realm/realm-core/issues) on our GitHub repository.

When filing an issue, please provide as much of the following information as possible in order to help others fix it:

1. **Goals**
2. **Expected results**
3. **Actual results**
4. **Steps to reproduce**
5. **Code sample that highlights the issue** (full project that we can compile ourselves are ideal)
6. **Version of Realm


## Contributing Enhancements

We love contributions to Realm! If you'd like to contribute code, documentation, or any other improvements, please [file a Pull Request](https://github.com/realm/realm-core/pulls) on our GitHub repository. Make sure to accept our [CLA](#cla) and to follow our [style guide](doc/development/coding_style_guide.cpp).

### Branches

Most changes should always go in the `master` branch - this means bugfixes, small enhancements, and features that do not introduce "behavior-breaking" changes. The exception is any code that touches the file format or sync protocol, as well as non-trivial changes to APIs - those should target the `breaking` branch. When in doubt please get in touch with the [Core Database team](https://github.com/orgs/realm/teams/core-database).
Occasionally a single PR, while a complete unit of work, does not contain everything needed to make a release. For example, adding a new data type involves work in the storage, query, schema, and sync layers which is usually shared by several contributors. In this case we use a feature integration branch. The current integration branch and its theme are announced with a pinned topic in [Discussions](https://github.com/realm/realm-core/discussions).

### Changelog

Pull requests that touch code should always add an entry to `CHANGELOG.md` with a sentence or two describing a change and a link to the relevant issue or the PR itself. The expectation is that the entry gives a general idea about the change, and then the PR or issue describes the nature of the change in more detail. Bug fixes should also mention the version the bug was introduced in.
Changelog entries should be high-level enough that they make sense when viewed by users of the Realm SDKs. A good example is 
```
 * Fixed an issue where opening a realm from two different processes on Windows results in a deadlock ([#9999](https://github.com/realm/realm-core/issues/9999), since vX.Y.Z) 
 ```
instead of `Fixed a bug with signaling win32 robust mutexes`. The nature of the change should be discussed in the PR or issue whereas the changelog entry describes the visible outcome to users. For anything else please use the `Internals` section of `CHANGELOG.md`.

### Commit Messages

Although we don’t enforce a strict format for commit messages, we prefer that you follow the guidelines below, which are common among open source projects. Following these guidelines helps with the review process, searching commit logs and documentation of implementation details. At a high level, the contents of the commit message should convey the rationale of the change, without delving into much detail.

Below are some guidelines about the format of the commit message itself:

* Separate the commit message into a single-line title and a separate body that describes the change.
* Make the title concise to be easily read within a commit log.
* Make the body concise, while including the complete reasoning. Unless required to understand the change, additional code examples or other details should be left to the pull request.
* If the commit fixes a bug, include the number of the issue in the message.
* Use the first person present tense - for example "Fix …" instead of "Fixes …" or "Fixed …".
* For text formatting and spelling, follow the same rules as documentation and in-code comments — for example, the use of capitalization and periods.
* If the commit is a bug fix on top of another recently committed change, or a revert or reapply of a patch, include the Git revision number of the prior related commit, e.g. `Revert abcd3fg because it caused #1234`.

### CLA

Realm welcomes all contributions! The only requirement we have is that, like many other projects, we need to have a [Contributor License Agreement](https://en.wikipedia.org/wiki/Contributor_License_Agreement) (CLA) in place before we can accept any external code. Our own CLA is a modified version of the Apache Software Foundation’s CLA.

[Please submit your CLA electronically using our Google form](https://docs.google.com/forms/d/e/1FAIpQLSeQ9ROFaTu9pyrmPhXc-dEnLD84DbLuT_-tPNZDOL9J10tOKQ/viewform) so we can accept your submissions. The GitHub username you file there will need to match that of your Pull Requests. If you have any questions or cannot file the CLA electronically, you can email <help@realm.io>.
