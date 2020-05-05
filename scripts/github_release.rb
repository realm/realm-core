#!/usr/bin/env ruby

require 'fileutils'
require 'pathname'
require 'tmpdir'

require 'octokit'

RELEASE =  ENV['GITHUB_VERSION']
REPOSITORY = ENV['GIHUB_REPO_PUBLISH']

dependencies = File.readlines('dependencies.list')
RELEASE_BODY = "Binaries available from s3://static.realm.io/downloads/sync.\n\n" + dependencies.join.strip

github = Octokit::Client.new
github.access_token = ENV['GITHUB_ACCESS_TOKEN']

puts 'Checking whether the Github release already exists'
previous_releases = github.releases(REPOSITORY).select { |release| release[:tag_name] == RELEASE }
if not previous_releases.empty?
	puts 'Deleting the existing Github release'
	github.delete_release(previous_releases.first[:url])
end

puts 'Creating GitHub release'
response = github.create_release(REPOSITORY, RELEASE, name: "Release #{RELEASE}", body: RELEASE_BODY,
                                 prerelease: false)
