#!groovy

def getSourceArchive() {
  checkout scm
  sh 'git clean -ffdx -e .????????'
  sh 'git submodule update --init'
}

def readGitTag() {
  sh "git describe --exact-match --tags HEAD | tail -n 1 > tag.txt 2>&1 || true"
  def tag = readFile('tag.txt').trim()
  return tag
}

def readGitSha() {
  sh "git rev-parse HEAD | cut -b1-8 > sha.txt"
  def sha = readFile('sha.txt').readLines().last().trim()
  return sha
}

def get_version(){
  def dependencies = readProperties file: 'dependencies.list'
  def gitTag = readGitTag()
  def gitSha = readGitSha()
  if (gitTag == "") {
    return "${dependencies.VERSION}-${gitSha}"
  }
  else {
    return "${dependencies.VERSION}"
  }
}

def doBuildPackage(distribution, fileType) {
  return {
    node('docker') {
      getSourceArchive()

      withCredentials([[$class: 'StringBinding', credentialsId: 'packagecloud-sync-devel-master-token', variable: 'PACKAGECLOUD_MASTER_TOKEN']]) {
        sh "sh packaging/package.sh ${distribution}"
      }
      
      dir('packaging/out') {
        step([$class: 'ArtifactArchiver', artifacts: "${distribution}/*.${fileType}", fingerprint: true])
        stash includes: "${distribution}/*.${fileType}", name: "packages-${distribution}"
      }
    }
  }
}

def doPublish(distribution, fileType, distroName, distroVersion) {
  return {
    node {
      getSourceArchive()
      packaging = load './packaging/publish.groovy'
      
      dir('packaging/out') {
        unstash "packages-${distribution}"
        dir(distribution) {
          packaging.uploadPackages('sync-devel', fileType, distroName, distroVersion, "*.${fileType}")
        }
      }
    }
  }
}

def doPublishGeneric() {
  return {
    node {
      getSourceArchive()
      def version = get_version()
      def topdir = pwd()
      dir('packaging/out') {
        unstash "packages-generic"
      }
      dir("core/v${version}/linux") {
        sh "mv ${topdir}/packaging/out/generic/realm-core-*.tgz ./"
      }

      step([
        $class: 'S3BucketPublisher',
        dontWaitForConcurrentBuildCompletion: false,
        entries: [[
          bucket: 'realm-ci-artifacts',
          excludedFile: '',
          flatten: false,
          gzipFiles: false,
          managedArtifacts: false,
          noUploadOnFailure: true,
          selectedRegion: 'us-east-1',
          sourceFile: "core/v${version}/linux/*.tgz",
          storageClass: 'STANDARD',
          uploadFromSlave: false,
          useServerSideEncryption: false
        ]],
        profileName: 'hub-jenkins-user',
        userMetadata: []
      ])
    }
  }
}

def setBuildName(newBuildName) {
  currentBuild.displayName = "${currentBuild.displayName} - ${newBuildName}"
}

def gitTag
def gitSha
def dependencies

stage 'check'
node('docker') {
  getSourceArchive()
  dependencies = readProperties file: 'dependencies.list'
  echo "VERSION: ${dependencies.VERSION}"

  gitTag = readGitTag()
  gitSha = readGitSha()
  echo "tag: ${gitTag}"
  if (gitTag == "") {
    echo "No tag given for this build"
    setBuildName("${gitSha}")
  } else {
    if (gitTag != "v${dependencies.VERSION}") {
      echo "Git tag '${gitTag}' does not match v${dependencies.VERSION}"
    } else {
      echo "Building release: '${gitTag}'"
      setBuildName("Tag ${gitTag}")
    }
  }

  rpmVersion = dependencies.VERSION.replaceAll("-", "_")
  echo "rpm version: ${rpmVersion}"
}

stage 'build-packages'
parallel(
  generic: doBuildPackage('generic', 'tgz'),
  centos7: doBuildPackage('centos-7', 'rpm'),
  centos6: doBuildPackage('centos-6', 'rpm'),
  ubuntu1604: doBuildPackage('ubuntu-1604', 'deb')
)

if (['next-major', 'ajl/build-ubuntu'].contains(env.BRANCH_NAME)) {
  stage 'publish-packages'
  parallel(
    generic: doPublishGeneric(),
    centos7: doPublish('centos-7', 'rpm', 'el', 7),
    centos6: doPublish('centos-6', 'rpm', 'el', 6),
    ubuntu1604: doPublish('ubuntu-1604', 'deb', 'ubuntu', 'xenial')
  )
  
  if (gitTag != "") {
    stage 'trigger release'
    build job: 'sync_release/realm-core-release',
      wait: false,
      parameters: [[$class: 'StringParameterValue', name: 'RPM_VERSION', value: "${rpmVersion}-${env.BUILD_NUMBER}"]]
  }
}