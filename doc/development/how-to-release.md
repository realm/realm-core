The core release process is automated with github [actions](https://github.com/realm/realm-core/actions)

1. Run the prepare-release action.
  - input the base branch that you would like to make a release from (usually this will be "master").
  - input the release version (eg. "10.14.7")

2. This will create a PR, which you should look over and get someone else on the team to review. Verify the changelog by checking the PR description which has a list of commits since the last tag was made. If any changes are required, commit them to this PR's branch.

3. When ready, merge the PR. You can squash if there are only "prepare release" changes, but use a "merge-commit" strategy if there are functional changes added manually to the PR. On merge, the "make-release" action will run. This takes care of:
  - Making a tag
  - Publishing the release on Github
  - Updating the Changelog
  - Announcing the release in the #appx-releases slack channel

## Previous process

The previous release notes documentation can be found on the old Realm wiki https://github.com/realm/realm-wiki/wiki/Releasing-Realm-Core
