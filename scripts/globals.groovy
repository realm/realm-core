#!groovy

import groovy.transform.Field

@Field String repoName = 'realm-sync'
@Field String version
@Field String gitSha
@Field String gitCommitHash
@Field String gitTag
@Field Map dependencies

def initialize() {
  dependencies = readProperties file: 'dependencies.list'

  sh "git describe --exact-match --tags HEAD | tail -n 1 > tag.txt 2>&1 || true"
  gitTag = readFile('tag.txt').trim()

  sh "git rev-parse HEAD | cut -b1-8 > sha.txt"
  gitSha = readFile('sha.txt').readLines().last().trim()

  gitCommitHash = sh(script: 'git rev-parse HEAD', returnStdout: true).trim()

  if (globals.gitTag == "") {
    version = "${dependencies.VERSION}-g${globals.gitSha}"
  }
  else {
    version = dependencies.VERSION
  }
}

return this;
