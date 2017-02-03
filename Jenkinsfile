#!groovy

def gitTag
def gitSha
def version
def dependencies
def isPublishingRun
def isPublishingLatestRun

timeout(time: 1, unit: 'HOURS') {
stage('gather-info') {
  node('docker') {
    checkout([
      $class: 'GitSCM',
      branches: scm.branches,
      gitTool: 'native git',
      extensions: scm.extensions + [[$class: 'CleanCheckout']],
      userRemoteConfigs: scm.userRemoteConfigs
    ])
    stash includes: '**', name: 'core-source'

    dependencies = readProperties file: 'dependencies.list'
    echo "VERSION: ${dependencies.VERSION}"

    gitTag = readGitTag()
    gitSha = readGitSha()
    version = get_version()
    echo "tag: ${gitTag}"
    if (gitTag == "") {
      echo "No tag given for this build"
      setBuildName(gitSha)
    } else {
      if (gitTag != "v${dependencies.VERSION}") {
        def message = "Git tag '${gitTag}' does not match v${dependencies.VERSION}"
        echo message
        throw new IllegalStateException(message)
      } else {
        echo "Building release: '${gitTag}'"
        setBuildName("Tag ${gitTag}")
      }
    }
  }

  isPublishingRun = gitTag != ""
  echo "Publishing Run: ${isPublishingRun}"

  rpmVersion = dependencies.VERSION.replaceAll("-", "_")
  echo "rpm version: ${rpmVersion}"

  if (['master'].contains(env.BRANCH_NAME)) {
    // If we're on master, instruct the docker image builds to push to the
    // cache registry
    env.DOCKER_PUSH = "1"
  }
}

stage 'check'

parallelExecutors = [
checkLinuxRelease: doBuildInDocker('check'),
checkLinuxDebug: doBuildInDocker('check-debug'),
//buildCocoa: doBuildCocoa(isPublishingRun),
buildNodeLinuxDebug: doBuildNodeInDocker('Debug', isPublishingRun),
buildNodeLinuxRelease: doBuildNodeInDocker('Release', isPublishingRun),
//buildNodeOsxStaticRelease: doBuildNodeInOsx('STATIC', 'Release', isPublishingRun),
//buildNodeOsxStaticDebug: doBuildNodeInOsx('STATIC', 'Debug', isPublishingRun),
//buildNodeOsxSharedRelease: doBuildNodeInOsx('SHARED', 'Release', isPublishingRun),
//buildNodeOsxSharedDebug: doBuildNodeInOsx('SHARED', 'Debug', isPublishingRun),
addressSanitizer: doBuildInDocker('jenkins-pipeline-address-sanitizer'),
buildWin32Release: doBuildWindows('Release', false, 'win32'),
buildUwpWin32Release: doBuildWindows('Release', true, 'win32'),
buildUwpWin64Release: doBuildWindows('Release', true, 'win64'),
packageGeneric: doBuildPackage('generic', 'tar.gz'),
packageCentos7: doBuildPackage('centos-7', 'rpm'),
packageCentos6: doBuildPackage('centos-6', 'rpm'),
packageUbuntu1604: doBuildPackage('ubuntu-1604', 'deb')
//buildUwpArmRelease: doBuildWindows('Release', true, 'arm')
//threadSanitizer: doBuildInDocker('jenkins-pipeline-thread-sanitizer')
]

androidAbis = ['armeabi-v7a', 'x86', 'mips', 'x86_64', 'arm64-v8a']
androidBuildTypes = ['Debug', 'Release']

for (def i = 0; i < androidAbis.size(); i++) {
  def abi = androidAbis[i]
  for (def j=0; j < androidBuildTypes.size(); j++) {
      def buildType = androidBuildTypes[j]
      parallelExecutors["android-${abi}-${buildType}"] = doAndroidBuildInDocker(abi, buildType)
  }
}

appleSdks = ['macosx',
             'iphoneos', 'iphonesimulator',
             'appletvos', 'appletvsimulator',
             'watchos', 'watchsimulator']
appleBuildTypes = ['MinSizeDebug', 'Release']

for (def i = 0; i < appleSdks.size(); i++) {
    def sdk = appleSdks[i]
    def tests = sdk == 'macosx' ? true : false
    for (def j = 0; j < appleBuildTypes.size(); j++) {
        def buildType = appleBuildTypes[j]
        parallelExecutors["${sdk}${buildType}"] = doBuildCocoa(sdk, buildType, tests)
    }
}


if (env.CHANGE_TARGET) {
    parallelExecutors['diffCoverage'] = buildDiffCoverage()
}

parallel parallelExecutors

stage('aggregate') {
  node('docker') {
      getArchive()
      for (def i = 0; i < androidAbis.size(); i++) {
          def abi = androidAbis[i]
          for (def j=0; j < androidBuildTypes.size(); j++) {
              def buildType = androidBuildTypes[j]
              unstash "install-${abi}-${buildType}"
          }
      }

      def buildEnv = docker.build 'realm-core:snapshot'
      def environment = environment()
      withEnv(environment) {
          buildEnv.inside {
              sh 'rake package-android'
          }
      }
  }
}

if (isPublishingRun) {
  stage('publish-packages') {
      parallel(
        generic: doPublishGeneric(),
               centos7: doPublish('centos-7', 'rpm', 'el', 7),
               centos6: doPublish('centos-6', 'rpm', 'el', 6),
               ubuntu1604: doPublish('ubuntu-1604', 'deb', 'ubuntu', 'xenial'),
               others: doPublishLocalArtifacts()
          )
  }
}
}

def buildDockerEnv(name) {
  docker.withRegistry("https://012067661104.dkr.ecr.eu-west-1.amazonaws.com", "ecr:eu-west-1:aws-ci-user") {
    env.DOCKER_REGISTRY = '012067661104.dkr.ecr.eu-west-1.amazonaws.com'
    sh "./packaging/docker_build.sh $name ."
  }

  return docker.image(name)
}

def doBuildCocoa(def isPublishingRun) {
  return {
    node('macos || osx_vegas') {
      getArchive()

      try {
        withEnv([
          'PATH=$PATH:/usr/local/bin',
          'REALM_ENABLE_ENCRYPTION=yes',
          'REALM_ENABLE_ASSERTIONS=yes',
          'MAKEFLAGS=CFLAGS_DEBUG\\=-Oz',
          'UNITTEST_SHUFFLE=1',
          'UNITTEST_REANDOM_SEED=random',
          'UNITTEST_XML=1',
          'UNITTEST_THREADS=1',
          'DEVELOPER_DIR=/Applications/Xcode-8.2.app/Contents/Developer/'
        ]) {
            sh '''
              dir=$(pwd)
              sh build.sh build-cocoa
              sh build.sh check-debug

              # Repack the release with just what we need so that it is not a 1 GB download
              version=$(sh build.sh get-version)
              tmpdir=$(mktemp -d /tmp/$$.XXXXXX) || exit 1
              (
                  cd $tmpdir || exit 1
                  unzip -qq "$dir/core-$version.zip" || exit 1
                  mv core/librealm-iphone.a core/librealm-ios.a
                  tar cf "$dir/realm-core-$version.tar.xz" --xz core || exit 1
              )
              rm -rf "$tmpdir" || exit 1

              cp realm-core-*.tar.xz realm-core-latest.tar.xz
            '''

            if (isPublishingRun) {
              stash includes: '*core-*.*.*.tar.xz', name: 'cocoa-package'
            }
	    archiveArtifacts artifacts: '*core-*.*.*.tar.xz'

            sh 'sh build.sh clean'
        }
      } finally {
        collectCompilerWarnings('clang', true)
        recordTests('check-debug-cocoa')
        withCredentials([[$class: 'FileBinding', credentialsId: 'c0cc8f9e-c3f1-4e22-b22f-6568392e26ae', variable: 's3cfg_config_file']]) {
          if (env.BRANCH_NAME == 'master') {
            sh 's3cmd -c $s3cfg_config_file put realm-core-latest.tar.xz s3://static.realm.io/downloads/core/'
          }
        }
      }
    }
  }
}

def doBuildInDocker(String command) {
  return {
    node('docker') {
      getArchive()

      def buildEnv = docker.build 'realm-core:snapshot'
      def environment = environment()
      withEnv(environment) {
        buildEnv.inside {
          try {
              sh "sh build.sh ${command}"
          } finally {
            collectCompilerWarnings('gcc', true)
            recordTests(command)
          }
        }
      }
    }
  }
}


def doAndroidBuildInDocker(String abi, String buildType) {
  return {
    node('docker') {
        sh 'rm -rf *'
      getArchive()

      def buildEnv = docker.build('realm-core-android:snapshot', '-f android.Dockerfile .')
      def environment = environment()
      withEnv(environment) {
        buildEnv.inside {
            try {
            sh "rake build-android-${abi}-${buildType}"
            stash includes: 'build.*/install/**', name: "install-${abi}-${buildType}"
          } finally {
            collectCompilerWarnings('gcc', true)
          }
        }
      }
    }
  }
}

def doBuildWindows(String buildType, boolean isUWP, String arch) {
    def cmakeDefinitions = isUWP ? '-DCMAKE_SYSTEM_NAME=WindowsStore -DCMAKE_SYSTEM_VERSION=10.0' : ''
    def archSuffix = ''
    if (arch == 'win64') {
        archSuffix = ' Win64'
    } else if (arch == 'arm') {
        archSuffix = ' ARM'
    }

    return {
        node('windows') {
            getArchive()

            dir('build-dir') {
                bat """
                    cmake ${cmakeDefinitions} -DREALM_BUILD_LIB_ONLY=1 -G \"Visual Studio 14 2015${archSuffix}\" -DCMAKE_BUILD_TYPE=${buildType} ..
                    cmake --build . --config ${buildType}
                    cpack -C ${buildType} -D CPACK_GENERATOR="TGZ"
                """
            }
        }
    }
}

def buildDiffCoverage() {
  return {
    node('docker') {
      checkout([
        $class: 'GitSCM',
        branches: scm.branches,
        gitTool: 'native git',
        extensions: scm.extensions + [[$class: 'CleanCheckout']],
        userRemoteConfigs: scm.userRemoteConfigs
      ])

      def buildEnv = buildDockerEnv('ci/realm-core:snapshot')
      def environment = environment()
      withEnv(environment) {
        buildEnv.inside {
          sh 'sh build.sh jenkins-pipeline-coverage'

          sh 'mkdir -p coverage'
          sh "diff-cover build.make.cover/gcovr.xml " +
            "--compare-branch=origin/${env.CHANGE_TARGET} " +
            "--html-report coverage/diff-coverage-report.html " +
            "| grep -F Coverage: " +
            "| head -n 1 " +
            "> diff-coverage"

          publishHTML(target: [
              allowMissing: false,
              alwaysLinkToLastBuild: false,
              keepAll: true,
              reportDir: 'coverage',
              reportFiles: 'diff-coverage-report.html',
              reportName: 'Diff Coverage'
          ])

          def coverageResults = readFile('diff-coverage')

          withCredentials([[$class: 'StringBinding', credentialsId: 'bot-github-token', variable: 'githubToken']]) {
              sh "curl -H \"Authorization: token ${env.githubToken}\" " +
                 "-d '{ \"body\": \"${coverageResults}\\n\\nPlease check your coverage here: ${env.BUILD_URL}Diff_Coverage\"}' " +
                 "\"https://api.github.com/repos/realm/realm-core/issues/${env.CHANGE_ID}/comments\""
          }
        }
      }
    }
  }
}

def doBuildNodeInDocker(String buildType, boolean isPublishingRun) {
  return {
    node('docker') {
      getArchive()

      def buildEnv = buildDockerEnv('ci/realm-core:snapshot')
      buildEnv.inside {
          try {
              sh """
                mkdir -p build_dir
                cd build_dir
                rm -rf *
                cmake -DREALM_ENABLE_ENCRYPTION=yes \\
                      -DREALM_ENABLE_ASSERTIONS=yes \\
                      -DREALM_BUILD_LIB_ONLY=1 \\
                      -DREALM_LIBTYPE=STATIC \\
                      -DCMAKE_BUILD_TYPE=${buildType} \\
                      -GNinja ..
                ninja
                cpack -D CPACK_GENERATOR="TGZ"
              """
              dir('build-dir') {
                  if (isPublishingRun) {
                      stash includes: 'realm-core-*.tar.gz', name: 'node'
                  }
                  archiveArtifacts artifacts: 'realm-core-*.tar.gz'
                  if (env.BRANCH_NAME == 'master') {
                    withCredentials([[$class: 'FileBinding', credentialsId: 'c0cc8f9e-c3f1-4e22-b22f-6568392e26ae', variable: 's3cfg_config_file']]) {
                      sh 's3cmd -c $s3cfg_config_file put realm-core-node-linux-latest.tar.gz s3://static.realm.io/downloads/core/'
                    }
                  }
              }
          } finally {
            collectCompilerWarnings('gcc', true)
          }
      }
    }
  }
}

def doBuildNodeInOsx(String libType, String buildType, boolean isPublishingRun) {
  return {
    node('macos || osx_vegas') {
      getArchive()

      try {
          sh """
                mkdir -p build_dir
                cd build_dir
                rm -rf *
                cmake -DREALM_ENABLE_ENCRYPTION=yes \\
                      -DREALM_ENABLE_ASSERTIONS=yes \\
                      -DREALM_BUILD_LIB_ONLY=1 \\
                      -DREALM_LIBTYPE=${libType} \\
                      -DCMAKE_BUILD_TYPE=${buildType} \\
                      -GNinja ..
                ninja
                cpack -D CPACK_GENERATOR="TGZ"
              """
          dir('build-dir') {
              if (isPublishingRun) {
                  stash includes: 'realm-core-*.tar.gz', name: 'node'
              }
              archiveArtifacts artifacts: 'realm-core-*.tar.gz'
          }
      } finally {
          collectCompilerWarnings('clang', true)
      }
    }
  }
}

def doBuildCocoa(String sdk, String buildType, boolean tests) {
    def testsDefinition = tests ? "" : "-D REALM_NO_TESTS=1"
    def skipSharedLib = tests ? "" : "-D REALM_SKIP_SHARED_LIB=1"

    return {
        node('macos || osx_vegas') {
            getArchive()

            try {
                sh """
                    mkdir build-dir
                    cd build-dir
                    cmake -D REALM_ENABLE_ENCRYPTION=yes \\
                          -D REALM_ENABLE_ASSERTIONS=yes \\
                          -D CMAKE_BUILD_TYPE=${buildType} \\
                          ${testsDefinition} ${skipSharedLib} -G Xcode ..
                    xcodebuild -sdk ${sdk} \\
                               -configuration ${buildType} \\
                               ONLY_ACTIVE_ARCH=NO
                    xcodebuild -sdk ${sdk} \\
                               -configuration ${buildType} \\
                               -target package \\
                               ONLY_ACTIVE_ARCH=NO
                """
            } finally {
                collectCompilerWarnings('clang', true)
            }
        }
    }
}

def recordTests(tag) {
    def tests = readFile('test/unit-test-report.xml')
    def modifiedTests = tests.replaceAll('realm-core-tests', tag)
    writeFile file: 'test/modified-test-report.xml', text: modifiedTests
    junit 'test/modified-test-report.xml'
}

def collectCompilerWarnings(compiler, fail) {
    def parserName
    if (compiler == 'gcc') {
        parserName = 'GNU Make + GNU C Compiler (gcc)'
    } else if ( compiler == 'clang' ) {
        parserName = 'Clang (LLVM based)'
    } else if ( compiler == 'msbuild' ) {
        parserName = 'MSBuild'
    }
    step([
        $class: 'WarningsPublisher',
        canComputeNew: false,
        canResolveRelativePaths: false,
        consoleParsers: [[parserName: parserName]],
        defaultEncoding: '',
        excludePattern: '',
        unstableTotalAll: fail?'0':'',
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
    return "${dependencies.VERSION}-g${gitSha}"
  }
  else {
    return "${dependencies.VERSION}"
  }
}

@NonCPS
def getDeviceNames(String commandOutput) {
  def deviceNames = []
  def lines = commandOutput.split('\n')
  for (i = 0; i < lines.size(); ++i) {
    if (lines[i].contains('\t')) {
      deviceNames << lines[i].split('\t')[0].trim()
    }
  }
  return deviceNames
}

def doBuildPackage(distribution, fileType) {
  return {
    node('docker') {
      getSourceArchive()

      docker.withRegistry("https://012067661104.dkr.ecr.eu-west-1.amazonaws.com", "ecr:eu-west-1:aws-ci-user") {
        env.DOCKER_REGISTRY = '012067661104.dkr.ecr.eu-west-1.amazonaws.com'
        withCredentials([[$class: 'StringBinding', credentialsId: 'packagecloud-sync-devel-master-token', variable: 'PACKAGECLOUD_MASTER_TOKEN']]) {
          sh "sh packaging/package.sh ${distribution}"
        }
      }

      dir('packaging/out') {
        archiveArtifacts artifacts: "${distribution}/*.${fileType}"
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
        sh "mv ${topdir}/packaging/out/generic/realm-core-*.tgz ./realm-core-${version}.tgz"
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

def doPublishLocalArtifacts() {
  // TODO create a Dockerfile for an image only containing s3cmd
  return {
    node('aws') {
      deleteDir()
      unstash 'cocoa-package'
      unstash 'node-linux-package'
      unstash 'node-cocoa-package'
      unstash 'android-package'
      unstash 'dylib-osx-package'
      unstash 'windows-package'

      withCredentials([[$class: 'FileBinding', credentialsId: 'c0cc8f9e-c3f1-4e22-b22f-6568392e26ae', variable: 's3cfg_config_file']]) {
        sh 'find . -type f -name "*.tar.*" -maxdepth 1 -exec s3cmd -c $s3cfg_config_file put {} s3://static.realm.io/downloads/core/ \\;'
        sh 'find . -type f -name "*.zip" -maxdepth 1 -exec s3cmd -c $s3cfg_config_file put {} s3://static.realm.io/downloads/core/ \\;'
      }
    }
  }
}

def setBuildName(newBuildName) {
  currentBuild.displayName = "${currentBuild.displayName} - ${newBuildName}"
}

def getArchive() {
    deleteDir()
    unstash 'core-source'
}

def getSourceArchive() {
  checkout scm
  sh 'git clean -ffdx -e .????????'
  sh 'git submodule update --init'
}
