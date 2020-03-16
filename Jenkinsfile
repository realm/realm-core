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
    } finally {
        deleteDir()
    }
  }
}

def withCustomRealmCloud(String version, String configPath, String appName, block = { it }) {
  def mdbRealmImage = docker.image("${env.DOCKER_REGISTRY}/ci/mongodb-realm-images:${version}")
  def stitchCliImage = docker.image("${env.DOCKER_REGISTRY}/ci/stitch-cli:190")
  docker.withRegistry("https://${env.DOCKER_REGISTRY}", "ecr:eu-west-1:aws-ci-user") {
    mdbRealmImage.pull()
    stitchCliImage.pull()
  }
  // run image, get IP
  withDockerNetwork { network ->
    mdbRealmImage.withRun("--network=${network} --network-alias=mongodb-realm") {
      stitchCliImage.inside("--network=${network}") {
        sh '''
          echo "Waiting for Stitch to start"
          while ! curl --output /dev/null --silent --head --fail http://mongodb-realm:9090; do
            sleep 1 && echo -n .;
          done;
        '''

        def access_token = sh(
          script: 'curl --request POST --header "Content-Type: application/json" --data \'{ "username":"unique_user@domain.com", "password":"password" }\' http://mongodb-realm:9090/api/admin/v3.0/auth/providers/local-userpass/login -s | jq ".access_token" -r',
          returnStdout: true
        ).trim()

        def group_id = sh(
          script: "curl --header 'Authorization: Bearer $access_token' http://mongodb-realm:9090/api/admin/v3.0/auth/profile -s | jq '.roles[0].group_id' -r",
          returnStdout: true
        ).trim()

        sh """
          /usr/bin/stitch-cli login --config-path=${configPath}/stitch-config --base-url=http://mongodb-realm:9090 --auth-provider=local-userpass --username=unique_user@domain.com --password=password
          /usr/bin/stitch-cli import --config-path=${configPath}/stitch-config --base-url=http://mongodb-realm:9090 --app-name auth-integration-tests --path=${configPath} --project-id $group_id -y --strategy replace
        """
      }
      block(network)
    }
  }
}


def doDockerBuild(String flavor, Boolean withCoverage, Boolean enableSync, String sanitizerFlags = "") {
  def sync = enableSync ? "sync" : ""
  def label = "${flavor}${enableSync ? '-sync' : ''}"
  def buildDir = "build"
  def cmakeFlags = ""
  if (withCoverage) {
    cmakeFlags += "-DCMAKE_BUILD_TYPE=Coverage "
  } else {
    cmakeFlags += "-DCMAKE_BUILD_TYPE=RelWithDebInfo "
  }
  cmakeFlags += sanitizerFlags;

  return {
    nodeWithSources('docker') {
      def image = buildDockerEnv("ci/realm-object-store:${flavor}", push: env.BRANCH_NAME == 'master')
      def buildSteps = { String dockerArgs = "" ->
        // The only reason we can't run commands directly is because sync builds need to
        // use CI's credentials to pull from the private repository. This can be removed
        // when sync is open sourced.
        sshagent(['realm-ci-ssh']) {
          image.inside("-v /etc/passwd:/etc/passwd:ro -v ${env.HOME}:${env.HOME} -v ${env.SSH_AUTH_SOCK}:${env.SSH_AUTH_SOCK} -e HOME=${env.HOME} ${dockerArgs}") {
            if (enableSync) {
              // sanity check the network to local stitch before continuing to compile everything
              sh "curl http://mongodb-realm:9090"
              def sourcesDir = pwd()
              cmakeFlags += "-DREALM_ENABLE_SYNC=1 -DREALM_ENABLE_AUTH_TESTS=1 -DREALM_MONGODB_ENDPOINT=\"http://mongodb-realm:9090\" -DREALM_STITCH_CONFIG=\"${sourcesDir}/tests/mongodb/stitch.json\" "
            }
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
      }

      if (enableSync) {
        // stitch images are auto-published internally to aws
        // after authenticating to aws (and ensure you are part of the realm-ecr-users permissions group) you can find the latest image by running:
        // `aws ecr describe-images --repository-name ci/mongodb-realm-images --query 'sort_by(imageDetails,& imagePushedAt)[-1].imageTags[0]'`
        withCustomRealmCloud("test_server-0ed2349a36352666402d0fb2e8763ac67731768c-race", "tests/mongodb", "auth-integration-tests") { networkName ->
          buildSteps("--network=${networkName}")
        }
      } else {
        buildSteps("")
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
          def image = buildDockerEnv('realm-object-store:android', extra_args: '-f android.Dockerfile', push: env.BRANCH_NAME == 'master')
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
