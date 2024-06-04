The core release process is automated with github [actions](https://github.com/realm/realm-core/actions)

1. Go to the prepare-release [action](https://github.com/realm/realm-core/actions/workflows/prepare-release.yml) and click the "Run workflow" dropdown.
  - Select the base branch that you would like to make a release from (usually this will be "master") in the drop down.
  - Enter the version of the new release (eg. "10.123.1" or "4.5.0-CustDemo")

2. This will create a PR, which you should look over and get someone else on the team to review. Verify the changelog by checking the PR description which has a list of commits since the last tag was made. If any changes are required, commit them to this PR's branch.

3. When ready, merge the PR. You can squash if there are only "prepare release" changes, but use a "merge-commit" strategy if there are functional changes added manually to the PR (note that if using a merge strategy, the last commit after merge will be the one tagged, so you may want to reorder the commits so that the 'prepare' commit comes last). On merge, the "make-release" action will run. This will:
  - Make a tag
  - Publish the release on Github
  - Open a PR to update the Changelog with the new template section
  - Announce the release in the #appx-releases slack channel

4. Find the newly generated PR that adds the new changelog section. Approve it and merge it.

## Previous process

The previous release notes documentation can be found on the old Realm wiki https://github.com/realm/realm-wiki/wiki/Releasing-Realm-Core
