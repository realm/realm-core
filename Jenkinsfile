#!groovy

@Library('realm-ci') _

// printing the timings is a placeholder until we can fix and integrate junit reporting via "-r junit"
testArguments = "-d=1"

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

def publishReport(String label) {
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
        steps()
    } finally {
        deleteDir()
    }
  }
}

def doDockerBuild(String flavor, Boolean withCoverage, Boolean enableSync, String sanitizerFlags = "") {
  def sync = enableSync ? "sync" : ""
  def label = "${flavor}${enableSync ? '-sync' : ''}"
  def buildDir = "build"
  def cmakeFlags = ""
  if (enableSync) {
    cmakeFlags += "-DREALM_ENABLE_SYNC=1 "
  }
  if (withCoverage) {
    cmakeFlags += "-DCMAKE_BUILD_TYPE=Coverage "
  } else {
    cmakeFlags += "-DCMAKE_BUILD_TYPE=RelWithDebInfo "
  }
  cmakeFlags += sanitizerFlags;

  return {
    nodeWithSources('docker') {
      def image = docker.build("ci/realm-object-store:${flavor}")
      // The only reason we can't run commands directly is because sync builds need to
      // use CI's credentials to pull from the private repository. This can be removed
      // when sync is open sourced.
      sshagent(['realm-ci-ssh']) {
        image.inside("-v /etc/passwd:/etc/passwd:ro -v ${env.HOME}:${env.HOME} -v ${env.SSH_AUTH_SOCK}:${env.SSH_AUTH_SOCK} -e HOME=${env.HOME}") {
          sh "cmake -B ${buildDir} -G Ninja ${cmakeFlags}"
          if (withCoverage) {
            sh "cmake --build ${buildDir} --target generate-coverage-cobertura" // builds and runs tests
            sh "cp ${buildDir}/coverage.xml coverage-${label}.xml"
          } else {
            sh "cmake --build ${buildDir} --target tests"
            sh "./${buildDir}/tests/tests ${testArguments}"
          }
        }
      }
      if (withCoverage) {
        echo "Stashing coverage-${label}"
        stash includes: "coverage-${label}.xml", name: "coverage-${label}"
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
          def image = docker.build('realm-object-store:ndk21', '-f android.Dockerfile .')
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

if (env.BRANCH_NAME == 'master') {
  env.DOCKER_PUSH = "1"
}

jobWrapper { // sets the max build time to 2 hours
  stage('prepare') {
    nodeWithSources('docker') {
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
      linux: doDockerBuild('linux', false, false),
      linux_sync: doDockerBuild('linux', true, true),
      macos_asan: doBuild('osx', 'macOS', true, '-DSANITIZE_ADDRESS=1'),
      // FIXME: the tsan build works, but it's reporting legit races that need to be fixed
      // see https://github.com/realm/realm-object-store/pull/905
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
      try {
        publishReport('linux-sync')
      } finally {
        deleteDir()
      }
    }
  }
} // jobWrapper
