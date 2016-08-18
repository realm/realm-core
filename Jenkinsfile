#!groovy

def gitTag
def gitSha
def dependencies

stage 'gather-info'
node {
  getSourceArchive()
  dependencies = readProperties file: 'dependencies.list'
  echo "VERSION: ${dependencies.VERSION}"

  gitTag = readGitTag()
  gitSha = readGitSha()
  echo "tag: ${gitTag}"
  if (gitTag == "") {
    echo "No tag given for this build"
    setBuildName(gitSha)
  } else {
    if (gitTag != "v${dependencies.VERSION}") {
      echo "Git tag '${gitTag}' does not match v${dependencies.VERSION}"
    } else {
      echo "Building release: '${gitTag}'"
      setBuildName("Tag ${gitTag}")
    }
  }
}

stage 'check'
parallel (
  checkLinuxRelease: doBuildInDocker('check'),
  checkLinuxDebug: doBuildInDocker('check-debug'),
  buildCocoa: doBuildCocoa(),
  buildNodeLinux: doBuildNodeInDocker(),
  buildNodeOsx: doBuildNodeInOsx()
)

stage 'build-packages'
parallel(
  generic: doBuildPackage('generic', 'tgz'),
  centos7: doBuildPackage('centos-7', 'rpm'),
  centos6: doBuildPackage('centos-6', 'rpm')
)

if (['ajl/jenkinsfile'].contains(env.BRANCH_NAME)) {
  stage 'publish-packages'
  parallel(
    generic: doPublishGeneric(),
    centos7: doPublish('centos-7', 'rpm', 'el', 7),
    centos6: doPublish('centos-6', 'rpm', 'el', 6)
  )

  if (gitTag != "") {
    stage 'trigger release'
    build job: 'sync_release/realm-core-rpm-release',
      wait: false,
      parameters: [[$class: 'StringParameterValue', name: 'RPM_VERSION', value: "${dependencies.VERSION}-${env.BUILD_NUMBER}"]]
  }
}

def doBuildCocoa() {
  return {
    node('osx') {
      checkout scm
      sh 'git clean -ffdx -e .????????'
      try {
        withEnv([
          'PATH=$PATH:/usr/local/bin',
          'DEVELOPER_DIR=/Applications/Xcode.app/Contents/Developer',
          'REALM_ENABLE_ENCRYPTION=yes',
          'REALM_ENABLE_ASSERTIONS=yes',
          'MAKEFLAGS=\'CFLAGS_DEBUG=-Oz\'',
          'UNITTEST_SHUFFLE=1',
          'UNITTEST_REANDOM_SEED=random',
          'UNITTEST_XML=1',
          'UNITTEST_THREADS=1'
        ]) {
            sh '''
              dir=$(pwd)
              sh build.sh config $dir/install
              sh build.sh build-cocoa
              sh build.sh check-debug

              # Repack the release with just what we need so that it's not a 1 GB download
              version=$(sh build.sh get-version)
              tmpdir=$(mktemp -d /tmp/$$.XXXXXX) || exit 1
              (
                  cd $tmpdir || exit 1
                  unzip -qq "$dir/core-$version.zip" || exit 1

                  # We only need an armv7s slice for CocoaPods, and the podspec never uses
                  # the debug build of core, so remove that slice
                  lipo -remove armv7s core/librealm-ios-dbg.a -o core/librealm-ios-dbg.a

                  tar cf "$dir/core-$version.tar.xz" --xz core || exit 1
              )
              rm -rf "$tmpdir" || exit 1

              cp core-*.tar.xz realm-core-latest.tar.xz
            '''
            archive '*core-*.*.*.tar.xz'
        }
      } finally {
        collectCompilerWarnings('clang')
        recordTests('check-debug-cocoa')
        withCredentials([[$class: 'FileBinding', credentialsId: 'c0cc8f9e-c3f1-4e22-b22f-6568392e26ae', variable: 's3cfg_config_file']]) {
          sh 's3cmd -c $s3cfg_config_file put realm-core-latest.tar.xz s3://static.realm.io/downloads/core'
        }
      }
    }
  }
}

def doBuildInDocker(String command) {
  return {
    node('docker') {
      checkout scm
      sh 'git clean -ffdx -e .????????'

      def buildEnv = docker.build 'realm-core:snapshot'
      def environment = environment()
      withEnv(environment) {
        buildEnv.inside {
          sh 'sh build.sh config'
          try {
              sh "sh build.sh ${command}"
          } finally {
            collectCompilerWarnings('gcc')
            recordTests(command)
          }
        }
      }
    }
  }
}

def doBuildNodeInDocker() {
  return {
    node('docker') {
      checkout scm
      sh 'git clean -ffdx -e .????????'

      def buildEnv = docker.build 'realm-core:snapshot'
      def environment = ['REALM_ENABLE_ENCRYPTION=yes', 'REALM_ENABLE_ASSERTIONS=yes']
      withEnv(environment) {
        buildEnv.inside {
          sh 'sh build.sh config'
          try {
              sh 'sh build.sh build-node-package'
              sh 'cp realm-core-node-*.tar.gz realm-core-node-linux-latest.tar.gz'
              archive '*realm-core-node-linux-*.*.*.tar.gz'
              withCredentials([[$class: 'FileBinding', credentialsId: 'c0cc8f9e-c3f1-4e22-b22f-6568392e26ae', variable: 's3cfg_config_file']]) {
                sh 's3cmd -c $s3cfg_config_file put realm-core-node-linux-latest.tar.gz s3://static.realm.io/downloads/core'
              }
          } finally {
            collectCompilerWarnings('gcc')
          }
        }
      }
    }
  }
}

def doBuildNodeInOsx() {
  return {
    node('osx') {
      checkout scm
      sh 'git clean -ffdx -e .????????'

      def environment = ['REALM_ENABLE_ENCRYPTION=yes', 'REALM_ENABLE_ASSERTIONS=yes']
      withEnv(environment) {
        sh 'sh build.sh config'
        try {
            sh 'sh build.sh build-node-package'
            sh 'cp realm-core-node-*.tar.gz realm-core-node-linux-osx.tar.gz'
            archive '*realm-core-node-osx-*.*.*.tar.gz'
            withCredentials([[$class: 'FileBinding', credentialsId: 'c0cc8f9e-c3f1-4e22-b22f-6568392e26ae', variable: 's3cfg_config_file']]) {
              sh 's3cmd -c $s3cfg_config_file put realm-core-node-osx-latest.tar.gz s3://static.realm.io/downloads/core'
            }
        } finally {
          collectCompilerWarnings('clang')
        }
      }
    }
  }
}

def recordTests(tag) {
    def tests = readFile('test/unit-test-report.xml')
    def modifiedTests = tests.replaceAll('DefaultSuite', tag)
    writeFile file: 'test/modified-test-report.xml', text: modifiedTests

    step([
        $class: 'XUnitBuilder',
        testTimeMargin: '3000',
        thresholdMode: 1,
        thresholds: [
        [
        $class: 'FailedThreshold',
        failureNewThreshold: '0',
        failureThreshold: '0',
        unstableNewThreshold: '0',
        unstableThreshold: '0'
        ], [
        $class: 'SkippedThreshold',
        failureNewThreshold: '0',
        failureThreshold: '0',
        unstableNewThreshold: '0',
        unstableThreshold: '0'
        ]
        ],
        tools: [[
        $class: 'UnitTestJunitHudsonTestType',
        deleteOutputFiles: true,
        failIfNotNew: true,
        pattern: 'test/modified-test-report.xml',
        skipNoTestFiles: false,
        stopProcessingIfError: true
        ]]
    ])
}

def collectCompilerWarnings(compiler) {
    def parserName
    if (compiler == 'gcc') {
        parserName = 'GNU Make + GNU C Compiler (gcc)'
    } else if ( compiler == 'clang' ) {
        parserName = 'Clang (LLVM based)'
    }
    step([
        $class: 'WarningsPublisher',
        canComputeNew: false,
        canResolveRelativePaths: false,
        consoleParsers: [[parserName: parserName]],
        defaultEncoding: '',
        excludePattern: '',
        failedTotalAll: '0',
        failedTotalHigh: '0',
        failedTotalLow: '0',
        failedTotalNormal: '0',
        healthy: '',
        includePattern: '',
        messagesPattern: '',
        unHealthy: ''
    ])
}

def environment() {
    return [
    "REALM_MAX_BPNODE_SIZE_DEBUG=4",
    "UNITTEST_SHUFFLE=1",
    "UNITTEST_RANDOM_SEED=random",
    "UNITTEST_THREADS=1",
    "UNITTEST_XML=1"
    ]
}

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

def get_version() {
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
        sh "mv ${topdir}/packaging/out/generic/realm-core-${version}.tgz realm-core-${version}.tgz"
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
