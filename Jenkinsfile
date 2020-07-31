#!groovy

@Library('realm-ci') _

// printing the timings is a placeholder until we can fix and integrate junit reporting via "-r junit"
testArguments = "-d=1"

dependencies = null

def readGitTag() {
  sh "git describe --exact-match --tags HEAD | tail -n 1 > tag.txt 2>&1 || true"
  return readFile('tag.txt').trim()
}

def readGitSha() {
  sh "git rev-parse HEAD | cut -b1-8 > sha.txt"
  return readFile('sha.txt').readLines().last().trim()
}

def publishCoverageReport(String label) {
  // Unfortunately, we cannot add a title or tag to individual coverage reports.
  echo "Unstashing coverage-${label}"
  unstash("coverage-${label}")

  step([
    $class: 'CoberturaPublisher',
    autoUpdateHealth: false,
    autoUpdateStability: false,
    coberturaReportFile: "coverage-${label}.xml",
    failNoReports: true,
    failUnhealthy: false,
    failUnstable: false,
    maxNumberOfBuilds: 0,
    onlyStable: false,
    sourceEncoding: 'ASCII',
    zoomCoverageChart: false
  ])
}

def nodeWithSources(String nodespec, Closure steps) {
  node(nodespec) {
    echo "Running on ${env.NODE_NAME} in ${env.WORKSPACE}"
    // Allocate a temp dir inside the workspace that will be deleted after the build
    String tempDir = "${env.WORKSPACE}/._tmp"

    try {
        withEnv([
            "TMP=${tempDir}",
            "TMPDIR=${tempDir}"
        ]) {
            // rlmCheckout will fetch object-store sources recursively and stash them,
            // so that every subsequent call in later stages only has to unstash them
            rlmCheckout(scm)
            // since checkout above also deletes everything, we need to create a tmp after checkout
            dir(tempDir) {
                touch '.workspace temp directory'
            }
            steps()
        }
    } finally {
        deleteDir()
    }
  }
}

def doDockerBuild(String flavor, Boolean enableSync, String sanitizerFlags = "") {
  def sync = enableSync ? "sync" : ""
  def label = "${flavor}${enableSync ? '-sync' : ''}"
  def buildDir = "build"
  def cmakeFlags = "-DCMAKE_BUILD_TYPE=RelWithDebInfo " + sanitizerFlags

  return {
    nodeWithSources('docker') {
      def image = buildDockerEnv("ci/realm-object-store:${flavor}", push: env.BRANCH_NAME == 'master')
      def sourcesDir = pwd()
      def buildSteps = { String dockerArgs = "" ->
        // The only reason we can't run commands directly is because sync builds need to
        // use CI's credentials to pull from the private repository. This can be removed
        // when sync is open sourced.
        sshagent(['realm-ci-ssh']) {
          image.inside("-v /etc/passwd:/etc/passwd:ro -v ${env.HOME}:${env.HOME} -v ${env.SSH_AUTH_SOCK}:${env.SSH_AUTH_SOCK} -e HOME=${env.HOME} ${dockerArgs}") {
            if (enableSync) {
              // sanity check the network to local stitch before continuing to compile everything
              sh "curl http://mongodb-realm:9090"
              cmakeFlags += "-DREALM_ENABLE_SYNC=1 -DREALM_ENABLE_AUTH_TESTS=1 -DREALM_MONGODB_ENDPOINT=\"http://mongodb-realm:9090\" -DREALM_STITCH_CONFIG=\"${sourcesDir}/tests/mongodb/stitch.json\" "
            }
            sh "cmake -B ${buildDir} -G Ninja ${cmakeFlags}"
            sh "cmake --build ${buildDir} --target tests"
            sh "./${buildDir}/tests/tests ${testArguments}"
          }
        }
      }

      if (enableSync) {
          // stitch images are auto-published every day to our CI
          // see https://github.com/realm/ci/tree/master/realm/docker/mongodb-realm
          // we refrain from using "latest" here to optimise docker pull cost due to a new image being built every day
          // if there's really a new feature you need from the latest stitch, upgrade this manually
        withRealmCloud(version: dependencies.MDBREALM_TEST_SERVER_TAG, appsToImport: ['auth-integration-tests': "${env.WORKSPACE}/tests/mongodb"]) { networkName ->
            buildSteps("--network=${networkName}")
        }
      } else {
        buildSteps("")
      }
    }
  }
}

def doAndroidDockerBuild() {
  return {
    node('docker') {
      try {
        rlmCheckout(scm)
        wrap([$class: 'AnsiColorBuildWrapper']) {
          def image = buildDockerEnv('ci/realm-object-store:android', extra_args: '-f android.Dockerfile', push: env.BRANCH_NAME == 'master')
          docker.image('tracer0tong/android-emulator').withRun { emulator ->
            image.inside("--link ${emulator.id}:emulator") {
              sh """
                cmake -B build -DREALM_PLATFORM=Android -DANDROID_NDK=\${ANDROID_NDK} -GNinja -DCMAKE_MAKE_PROGRAM=ninja
                cmake --build build
                adb connect emulator
                timeout 10m adb wait-for-device
                adb push build/tests/tests /data/local/tmp
                adb shell '/data/local/tmp/tests || echo __ADB_FAIL__' | tee adb.log
                ! grep __ADB_FAIL__ adb.log
              """
            }
          }
        }
      } finally {
          deleteDir()
      }
    }
  }
}

def doBuild(String nodeSpec, String flavor, Boolean enableSync, String sanitizerFlags = "") {
  def sync = enableSync ? "sync" : "false"
  def label = "${flavor}${enableSync ? '-sync' : ''}"
  def buildDir = "build"
  def cmakeFlags = "-DCMAKE_CXX_FLAGS=-Werror "
  if (enableSync) {
    cmakeFlags += "-DREALM_ENABLE_SYNC=1 "
  }
  cmakeFlags += sanitizerFlags;

  return {
    nodeWithSources(nodeSpec) {
      sh "cmake -B ${buildDir} -G Ninja ${cmakeFlags} && cmake --build ${buildDir}"
      sh "${buildDir}/tests/tests ${testArguments}"
    }
  }
}

def doWindowsBuild() {
  return {
    nodeWithSources('windows') {
      bat """
        "${tool 'cmake'}" . -DCMAKE_SYSTEM_VERSION="8.1"
        "${tool 'cmake'}" --build . --config Release
        tests\\Release\\tests.exe ${testArguments}
      """
    }
  }
}

def doWindowsUniversalBuild() {
  return {
    nodeWithSources('windows') {
      bat """
        "${tool 'cmake'}" . -DCMAKE_SYSTEM_NAME="WindowsStore" -DCMAKE_SYSTEM_VERSION="10.0"
        "${tool 'cmake'}" --build . --config Release --target realm-object-store
      """
    }
  }
}

def setBuildName(newBuildName) {
  currentBuild.displayName = "${currentBuild.displayName} - ${newBuildName}"
}

jobWrapper { // sets the max build time to 2 hours
  stage('prepare') {
    nodeWithSources('docker') {
      dependencies = readProperties(file: 'dependencies.list')
      gitTag = readGitTag()
      gitSha = readGitSha()
      echo "tag: ${gitTag}"
      if (gitTag == "") {
        echo "No tag given for this build"
        setBuildName("${gitSha}")
      } else {
        echo "Building release: '${gitTag}'"
        setBuildName("Tag ${gitTag}")
      }
    }
  }

  stage('unit-tests') {
    parallel(
      linux: doDockerBuild('linux', false),
      linux_sync: doDockerBuild('linux', true),
      linux_asan: doDockerBuild('linux', true, '-DSANITIZE_ADDRESS=1'),
      macos_asan: doBuild('osx', 'macOS', true, '-DSANITIZE_ADDRESS=1'),
      // FIXME: the tsan build works, but it's reporting legit races that need to be fixed
      // see https://github.com/realm/realm-object-store/pull/905
      // linux_tsan: doDockerBuild('linux', true, '-DSANITIZE_THREAD=1'),
      // macos_tsan: doBuild('osx', 'macOS', true, '-DSANITIZE_THREAD=1'),
      android: doAndroidDockerBuild(),
      macos: doBuild('osx', 'macOS', false),
      macos_sync: doBuild('osx', 'macOS', true),
      win32: doWindowsBuild(),
      windows_universal: doWindowsUniversalBuild()
    )
    currentBuild.result = 'SUCCESS'
  }

  stage('publish') {
    node('docker') {
      // we need sources to allow the coverage report to display them
      rlmCheckout(scm)
        // coverage reports assume sources are in the parent directory
        dir("build") {
          publishCoverageReport('macOS-sync')
        }
    }
  }
} // jobWrapper
