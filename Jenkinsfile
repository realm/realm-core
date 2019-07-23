#!groovy

@Library('realm-ci') _

cocoaStashes = []
androidStashes = []
publishingStashes = []

tokens = "${env.JOB_NAME}".tokenize('/')
org = tokens[tokens.size()-3]
repo = tokens[tokens.size()-2]
branch = tokens[tokens.size()-1]
valgrindCheck = true // FIXME: default to false

jobWrapper {
    stage('gather-info') {
        node('docker') {
            getSourceArchive()
            stash includes: '**', name: 'core-source', useDefaultExcludes: false

            dependencies = readProperties file: 'dependencies.list'
            echo "Version in dependencies.list: ${dependencies.VERSION}"

            gitTag = readGitTag()
            gitSha = sh(returnStdout: true, script: 'git rev-parse HEAD').trim().take(8)
            gitDescribeVersion = sh(returnStdout: true, script: 'git describe --tags').trim()

            echo "Git tag: ${gitTag ?: 'none'}"
            if (!gitTag) {
                echo "No tag given for this build"
                setBuildName(gitSha)
            } else {
                if (gitTag != "v${dependencies.VERSION}") {
                    error "Git tag '${gitTag}' does not match v${dependencies.VERSION}"
                } else {
                    echo "Building release: '${gitTag}'"
                    setBuildName("Tag ${gitTag}")
                }
            }
        }

        isPullRequest = !!env.CHANGE_TARGET
        targetBranch = isPullRequest ? env.CHANGE_TARGET : "none"
        currentBranch = env.BRANCH_NAME
        println "Building branch: ${currentBranch}"
        println "Target branch: ${targetBranch}"

        releaseTesting = targetBranch.contains('release')
        isPublishingRun = false
        if (gitTag) {
            isPublishingRun = currentBranch.contains('release')
        }

        echo "Pull request: ${isPullRequest ? 'yes' : 'no'}"
        echo "Release Run: ${releaseTesting ? 'yes' : 'no'}"
        echo "Publishing Run: ${isPublishingRun ? 'yes' : 'no'}"

        if (['master'].contains(env.BRANCH_NAME)) {
            // If we're on master, instruct the docker image builds to push to the
            // cache registry
            env.DOCKER_PUSH = "1"
            valgrindCheck = true
        }
    }

    stage('Checking') {
        parallelExecutors = [
            checkLinuxDebug         : doCheckInDocker('Debug')
           // checkLinuxDebugNoEncryp : doCheckInDocker('Debug', '4', 'OFF'),
           // checkMacOsRelease       : doBuildMacOs('Release', true),
           // checkWin32Debug         : doBuildWindows('Debug', false, 'Win32', true),
           // checkWin64Release       : doBuildWindows('Release', false, 'x64', true),
           // iosDebug                : doBuildAppleDevice('ios', 'MinSizeDebug'),
           // androidArm64Debug       : doAndroidBuildInDocker('arm64-v8a', 'Debug', false),
           // threadSanitizer         : doCheckSanity('Debug', '1000', 'thread'),
           // addressSanitizer        : doCheckSanity('Debug', '1000', 'address')
        ]
        if (releaseTesting) {
            extendedChecks = [
                checkLinuxRelease       : doCheckInDocker('Release'),
                checkMacOsDebug         : doBuildMacOs('Debug', true),
                buildUwpx64Debug        : doBuildWindows('Debug', true, 'x64', false),
                androidArmeabiRelease   : doAndroidBuildInDocker('armeabi-v7a', 'Release', true),
                coverage                : doBuildCoverage(),
                performance             : buildPerformance()
            ]
            parallelExecutors.putAll(extendedChecks)
        }
        parallel parallelExecutors
    }

    if (isPublishingRun) {
        stage('BuildPackages') {
            parallelExecutors = [
                buildMacOsDebug     : doBuildMacOs('Debug', false),
                buildMacOsRelease   : doBuildMacOs('Release', false),

                buildWin32Debug     : doBuildWindows('Debug', false, 'Win32', false),
                buildWin32Release   : doBuildWindows('Release', false, 'Win32', false),
                buildWin64Debug     : doBuildWindows('Debug', false, 'x64', false),
                buildWin64Release   : doBuildWindows('Release', false, 'x64', false),
                buildUwpWin32Debug  : doBuildWindows('Debug', true, 'Win32', false),
                buildUwpWin32Release: doBuildWindows('Release', true, 'Win32', false),
                buildUwpx64Debug    : doBuildWindows('Debug', true, 'x64', false),
                buildUwpx64Release  : doBuildWindows('Release', true, 'x64', false),
                buildUwpArmDebug    : doBuildWindows('Debug', true, 'ARM', false),
                buildUwpArmRelease  : doBuildWindows('Release', true, 'ARM', false),

                buildLinuxDebug     : doBuildLinux('Debug'),
                buildLinuxRelease   : doBuildLinux('Release'),
                buildLinuxRelAssert : doBuildLinux('RelAssert'),
                buildLinuxASAN      : doBuildLinuxClang("RelASAN"),
                buildLinuxTSAN      : doBuildLinuxClang("RelTSAN")
            ]

            androidAbis = ['armeabi-v7a', 'x86', 'mips', 'x86_64', 'arm64-v8a']
            androidBuildTypes = ['Debug', 'Release']

            for (abi in androidAbis) {
                for (buildType in androidBuildTypes) {
                    parallelExecutors["android-${abi}-${buildType}"] = doAndroidBuildInDocker(abi, buildType, false)
                }
            }

            appleSdks = ['ios', 'tvos', 'watchos']
            appleBuildTypes = ['MinSizeDebug', 'Release']

            for (sdk in appleSdks) {
                for (buildType in appleBuildTypes) {
                    parallelExecutors["${sdk}${buildType}"] = doBuildAppleDevice(sdk, buildType)
                }
            }

            parallel parallelExecutors
        }
        stage('Aggregate') {
            parallel (
                cocoa: {
                    node('docker') {
                        getArchive()
                        for (cocoaStash in cocoaStashes) {
                            unstash name: cocoaStash
                        }
                        sh 'tools/build-cocoa.sh'
                        archiveArtifacts('realm-core-cocoa*.tar.xz')
                        def stashName = 'cocoa'
                        stash includes: 'realm-core-cocoa*.tar.xz', name: stashName
                        publishingStashes << stashName
                    }
                },
                android: {
                    node('docker') {
                        getArchive()
                        for (androidStash in androidStashes) {
                            unstash name: androidStash
                        }
                        sh 'tools/build-android.sh'
                        archiveArtifacts('realm-core-android*.tar.gz')
                        def stashName = 'android'
                        stash includes: 'realm-core-android*.tar.gz', name: stashName
                        publishingStashes << stashName
                    }
                }
            )
        }
        stage('publish-packages') {
            parallel(
                others: doPublishLocalArtifacts()
            )
        }
    }
}

timeout(time: 6, unit: 'HOURS') {
    if (valgrindCheck) {
        stage('Valgrind') {
            parallel(
                valgrind: doCheckValgrind()
            )
        }
    }
}

def doCheckInDocker(String buildType, String maxBpNodeSize = '1000', String enableEncryption = 'ON') {
    return {
        node('docker') {
            getArchive()
            def buildEnv = docker.build 'realm-core:snapshot'
            def environment = environment()
            environment << 'UNITTEST_PROGRESS=1'
            withEnv(environment) {
                buildEnv.inside() {
                    try {
                        sh """
                           mkdir build-dir
                           cd build-dir
                           cmake -D CMAKE_BUILD_TYPE=${buildType} -D REALM_MAX_BPNODE_SIZE=${maxBpNodeSize} -DREALM_ENABLE_ENCRYPTION=${enableEncryption} -G Ninja ..
                        """
                        runAndCollectWarnings(script: "cd build-dir && ninja")
                        sh """
                           cd build-dir/test
                           ./realm-tests
                        """
                    } finally {
                        recordTests("Linux-${buildType}")
                    }
                }
            }
        }
    }
}

def doCheckSanity(String buildType, String maxBpNodeSize = '1000', String sanitizeMode='') {
    return {
        node('docker') {
            getArchive()
            def buildEnv = docker.build('realm-core-clang:snapshot', '-f clang.Dockerfile .')
            def environment = environment()
            def sanitizeFlags = ''
            def privileged = '';
            environment << 'UNITTEST_PROGRESS=1'
            if (sanitizeMode.contains('thread')) {
                sanitizeFlags = '-D REALM_TSAN=ON'
            } else if (sanitizeMode.contains('address')) {
                privileged = '--privileged'
                sanitizeFlags = '-D REALM_ASAN=ON'
            }
            withEnv(environment) {
                buildEnv.inside(privileged) {
                    try {
                        sh """
                           mkdir build-dir
                           cd build-dir
                           cmake -D CMAKE_BUILD_TYPE=${buildType} -D REALM_MAX_BPNODE_SIZE=${maxBpNodeSize} ${sanitizeFlags} -G Ninja ..
                        """
                        runAndCollectWarnings(script: "cd build-dir && ninja")
                        sh """
                           cd build-dir/test
                           ./realm-tests
                        """
                    } finally {
                        recordTests("Linux-${buildType}")
                    }
                }
            }
        }
    }
}

def doBuildLinux(String buildType) {
    return {
        node('docker') {
            getSourceArchive()

            docker.build('realm-core-generic:snapshot', '-f generic.Dockerfile .').inside {
                sh """
                   cmake --help
                   rm -rf build-dir
                   mkdir build-dir
                   cd build-dir
                   scl enable devtoolset-6 -- cmake -DCMAKE_BUILD_TYPE=${buildType} -DREALM_NO_TESTS=1 ..
                   make -j4
                   cpack -G TGZ
                """
            }

            dir('build-dir') {
                archiveArtifacts("*.tar.gz")
                def stashName = "linux___${buildType}"
                stash includes:"*.tar.gz", name:stashName
                publishingStashes << stashName
            }
        }
    }
}

def doBuildLinuxClang(String buildType) {
    return {
        node('docker') {
            getArchive()
            docker.build('realm-core-clang:snapshot', '-f clang.Dockerfile .').inside() {
                sh """
                   mkdir build-dir
                   cd build-dir
                   cmake -D CMAKE_BUILD_TYPE=${buildType} -DREALM_NO_TESTS=1 -G Ninja ..
                   ninja
                   cpack -G TGZ
                """
            }
            dir('build-dir') {
                archiveArtifacts("*.tar.gz")
                def stashName = "linux___${buildType}"
                stash includes:"*.tar.gz", name:stashName
                publishingStashes << stashName
            }
        }
    }
}

def doCheckValgrind() {
    return {
        node('docker') {
            getArchive()
            def buildEnv = docker.build('realm-core-valgrind:snapshot', '-f valgrind.Dockerfile .')
            def environment = environment()
            environment << 'UNITTEST_PROGRESS=1'
            withEnv(environment) {
                buildEnv.inside {
                    def workspace = pwd()
                    try {
                        sh """
                           mkdir build-dir
                           cd build-dir
                           cmake -D CMAKE_BUILD_TYPE=RelWithDebInfo -D REALM_VALGRIND=ON -D REALM_ENABLE_ALLOC_SET_ZERO=ON -D REALM_MAX_BPNODE_SIZE=1000 -G Ninja ..
                        """
                        runAndCollectWarnings(script: "cd build-dir && ninja")
                        sh """
                            cd build-dir/test
                            valgrind --version
                            valgrind --tool=memcheck --leak-check=full --undef-value-errors=yes --track-origins=yes --child-silent-after-fork=no --trace-children=yes --suppressions=${workspace}/test/valgrind.suppress --error-exitcode=1 ./realm-tests --no-error-exitcode
                        """
                    } finally {
                        recordTests("Linux-ValgrindDebug")
                    }
                }
            }
        }
    }
}

def doAndroidBuildInDocker(String abi, String buildType, boolean runTestsInEmulator) {
    def cores = 4
    return {
        node('docker') {
            getArchive()
            def stashName = "android___${abi}___${buildType}"
            def buildDir = "build-${stashName}".replaceAll('___', '-')
            def buildEnv = docker.build('realm-core-android:snapshot', '-f android.Dockerfile .')
            def environment = environment()
            environment << 'UNITTEST_PROGRESS=1'
            withEnv(environment) {
                if(!runTestsInEmulator) {
                    buildEnv.inside {
                        runAndCollectWarnings(script: "tools/cross_compile.sh -o android -a ${abi} -t ${buildType} -v ${gitDescribeVersion}")
                        dir(buildDir) {
                            archiveArtifacts('realm-*.tar.gz')
                        }
                        stash includes:"${buildDir}/realm-*.tar.gz", name:stashName
                        androidStashes << stashName
                        if (gitTag) {
                            publishingStashes << stashName
                        }
                    }
                } else {
                    docker.image('tracer0tong/android-emulator').withRun('-e ARCH=armeabi-v7a') { emulator ->
                        buildEnv.inside("--link ${emulator.id}:emulator") {
                            runAndCollectWarnings(script: "tools/cross_compile.sh -o android -a ${abi} -t ${buildType} -v ${gitDescribeVersion}")
                            dir(buildDir) {
                                archiveArtifacts('realm-*.tar.gz')
                            }
                            stash includes:"${buildDir}/realm-*.tar.gz", name:stashName
                            androidStashes << stashName
                            if (gitTag) {
                                publishingStashes << stashName
                            }
                            try {
                                sh '''
                                   cd $(find . -type d -maxdepth 1 -name build-android*)
                                   adb connect emulator
                                   timeout 10m adb wait-for-device
                                   adb push test/realm-tests /data/local/tmp
                                   find test -type f -name "*.json" -maxdepth 1 -exec adb push {} /data/local/tmp \\;
                                   find test -type f -name "*.realm" -maxdepth 1 -exec adb push {} /data/local/tmp \\;
                                   find test -type f -name "*.txt" -maxdepth 1 -exec adb push {} /data/local/tmp \\;
                                   adb shell \'cd /data/local/tmp; UNITTEST_PROGRESS=1 ./realm-tests || echo __ADB_FAIL__\' | tee adb.log
                                   ! grep __ADB_FAIL__ adb.log
                               '''
                            } finally {
                                sh '''
                                   mkdir -p build-dir/test
                                   cd build-dir/test
                                   adb pull /data/local/tmp/unit-test-report.xml
                                '''
                                recordTests('android')
                            }
                        }
                    }
                }
            }
        }
    }
}

def doBuildWindows(String buildType, boolean isUWP, String platform, boolean runTests) {
    def cmakeDefinitions;
    if (isUWP) {
      cmakeDefinitions = '-DCMAKE_SYSTEM_NAME=WindowsStore -DCMAKE_SYSTEM_VERSION=10.0 -DREALM_BUILD_LIB_ONLY=1'
    } else {
      cmakeDefinitions = '-DCMAKE_SYSTEM_VERSION=8.1'
    }
    if (!runTests) {
      cmakeDefinitions += ' -DREALM_NO_TESTS=1'
    }

    return {
        node('windows') {
            getArchive()

            dir('build-dir') {
                bat "\"${tool 'cmake'}\" ${cmakeDefinitions} -D CMAKE_GENERATOR_PLATFORM=${platform} -D CPACK_SYSTEM_NAME=${isUWP?'UWP':'Windows'}-${platform} -D CMAKE_BUILD_TYPE=${buildType} .."
                withEnv(["_MSPDBSRV_ENDPOINT_=${UUID.randomUUID().toString()}"]) {
                    runAndCollectWarnings(parser: 'msbuild', isWindows: true, script: "\"${tool 'cmake'}\" --build . --config ${buildType}")
                }
                bat "\"${tool 'cmake'}\\..\\cpack.exe\" -C ${buildType} -D CPACK_GENERATOR=TGZ"
                if (gitTag) {
                    def stashName = "windows___${platform}___${isUWP?'uwp':'nouwp'}___${buildType}"
                    archiveArtifacts('*.tar.gz')
                    stash includes:'*.tar.gz', name:stashName
                    publishingStashes << stashName
                }
            }
            if (runTests) {
                def environment = environment() << "TMP=${env.WORKSPACE}\\temp"
                environment << 'UNITTEST_PROGRESS=1'
                withEnv(environment) {
                    dir("build-dir/test/${buildType}") {
                        bat '''
                          mkdir %TMP%
                          realm-tests.exe --no-error-exit-code
                          rmdir /Q /S %TMP%
                        '''
                    }
                }
                recordTests("Windows-${platform}-${buildType}")
            }
        }
    }
}


def buildPerformance() {
  return {
    // Select docker-cph-X.  We want docker, metal (brix) and only one executor
    // (exclusive), if the machine changes also change REALM_BENCH_MACHID below
    node('docker && brix && exclusive') {
      getArchive()

      // REALM_BENCH_DIR tells the gen_bench_hist.sh script where to place results
      // REALM_BENCH_MACHID gives the results an id - results are organized by hardware to prevent mixing cached results with runs on different machines
      // MPLCONFIGDIR gives the python matplotlib library a config directory, otherwise it will try to make one on the user home dir which fails in docker
      docker.build('realm-core:snapshot').inside {
        withEnv(["REALM_BENCH_DIR=${env.WORKSPACE}/test/bench/core-benchmarks", "REALM_BENCH_MACHID=docker-brix","MPLCONFIGDIR=${env.WORKSPACE}/test/bench/config"]) {
          rlmS3Get file: 'core-benchmarks.zip', path: 'downloads/core/core-benchmarks.zip'
          sh 'unzip core-benchmarks.zip -d test/bench/'
          sh 'rm core-benchmarks.zip'

          sh """
            cd test/bench
            mkdir -p core-benchmarks results
            ./gen_bench_hist.sh origin/${env.CHANGE_TARGET}
            ./parse_bench_hist.py --local-html results/ core-benchmarks/
          """
          zip dir: 'test/bench', glob: 'core-benchmarks/**/*', zipFile: 'core-benchmarks.zip'
          rlmS3Put file: 'core-benchmarks.zip', path: 'downloads/core/core-benchmarks.zip'
          publishHTML(target: [allowMissing: false, alwaysLinkToLastBuild: false, keepAll: true, reportDir: 'test/bench/results', reportFiles: 'report.html', reportName: 'Performance Report'])
          withCredentials([[$class: 'StringBinding', credentialsId: 'bot-github-token', variable: 'githubToken']]) {
              sh "curl -H \"Authorization: token ${env.githubToken}\" " +
                 "-d '{ \"body\": \"Check the performance result here: ${env.BUILD_URL}Performance_Report\"}' " +
                 "\"https://api.github.com/repos/realm/${repo}/issues/${env.CHANGE_ID}/comments\""
          }
        }
      }
    }
  }
}

def doBuildMacOs(String buildType, boolean runTests) {
    def sdk = 'macosx'
    return {
        node('osx') {
            getArchive()

            def buildTests = runTests ? '' : '-DREALM_NO_TESTS=1'

            dir("build-macos-${buildType}") {
                withEnv(['DEVELOPER_DIR=/Applications/Xcode-9.2.app/Contents/Developer/']) {
                    // This is a dirty trick to work around a bug in xcode
                    // It will hang if launched on the same project (cmake trying the compiler out)
                    // in parallel.
                    retry(3) {
                        timeout(time: 2, unit: 'MINUTES') {
                            sh """
                                    rm -rf *
                                    cmake -D CMAKE_TOOLCHAIN_FILE=../tools/cmake/macos.toolchain.cmake \\
                                          -D CMAKE_BUILD_TYPE=${buildType} \\
                                          -D REALM_VERSION=${gitDescribeVersion} \\
                                          ${buildTests} -G Xcode ..
                                """
                        }
                    }

                    runAndCollectWarnings(parser: 'clang', script: """
                            xcodebuild -sdk macosx \\
                                       -configuration ${buildType} \\
                                       -target package \\
                                       ONLY_ACTIVE_ARCH=NO
                            """)
                }
            }

            archiveArtifacts("build-macos-${buildType}/*.tar.gz")

            def stashName = "macos___${buildType}"
            stash includes:"build-macos-${buildType}/*.tar.gz", name:stashName
            cocoaStashes << stashName
            publishingStashes << stashName

            if (runTests) {
                try {
                    dir("build-macos-${buildType}") {
                        def environment = environment()
                        environment << 'UNITTEST_PROGRESS=1'
                        withEnv(environment) {
                        sh """
                            cd test
                            ./${buildType}/realm-tests.app/Contents/MacOS/realm-tests
                            cp $TMPDIR/unit-test-report.xml .
                        """
                        }
                    }
                } finally {
                    // recordTests expects the test results xml file in a build-dir/test/ folder
                    sh """
                        mkdir -p build-dir/test
                        cp build-macos-${buildType}/test/unit-test-report.xml build-dir/test/
                    """
                    recordTests("macos_${buildType}")
                }
            }
        }
    }
}

def doBuildAppleDevice(String sdk, String buildType) {
    return {
        node('osx') {
            getArchive()

            withEnv(['DEVELOPER_DIR=/Applications/Xcode-9.2.app/Contents/Developer/',
                     'XCODE10_DEVELOPER_DIR=/Applications/Xcode-10.app/Contents/Developer/']) {
                retry(3) {
                    timeout(time: 15, unit: 'MINUTES') {
                        runAndCollectWarnings(parser:'clang', script: """
                                rm -rf build-*
                                tools/cross_compile.sh -o ${sdk} -t ${buildType} -v ${gitDescribeVersion}
                            """)
                    }
                }
            }
            archiveArtifacts("build-${sdk}-${buildType}/*.tar.gz")
            def stashName = "${sdk}___${buildType}"
            stash includes:"build-${sdk}-${buildType}/*.tar.gz", name:stashName
            cocoaStashes << stashName
            if(gitTag) {
                publishingStashes << stashName
            }
        }
    }
}

def doBuildCoverage() {
  return {
    node('docker') {
      getArchive()
      docker.build('realm-core:snapshot').inside {
        def workspace = pwd()
        sh """
          mkdir build
          cd build
          cmake -G Ninja -D REALM_COVERAGE=ON ..
          ninja
          cd ..
          lcov --no-external --capture --initial --directory . --output-file ${workspace}/coverage-base.info
          cd build/test
          ulimit -c unlimited
          UNITTEST_PROGRESS=1 ./realm-tests
          cd ../..
          lcov --no-external --directory . --capture --output-file ${workspace}/coverage-test.info
          lcov --add-tracefile ${workspace}/coverage-base.info --add-tracefile coverage-test.info --output-file ${workspace}/coverage-total.info
          lcov --remove ${workspace}/coverage-total.info '/usr/*' '${workspace}/test/*' --output-file ${workspace}/coverage-filtered.info
          rm coverage-base.info coverage-test.info coverage-total.info
        """
        withCredentials([[$class: 'StringBinding', credentialsId: 'codecov-token-core', variable: 'CODECOV_TOKEN']]) {
          sh '''
            curl -s https://codecov.io/bash | bash
          '''
        }
      }
    }
  }
}

/**
 *  Wraps the test recorder by adding a tag which will make the test distinguishible
 */
def recordTests(tag) {
    def tests = readFile('build-dir/test/unit-test-report.xml')
    def modifiedTests = tests.replaceAll('realm-core-tests', tag)
    writeFile file: 'build-dir/test/modified-test-report.xml', text: modifiedTests
    junit 'build-dir/test/modified-test-report.xml'
}

def environment() {
    return [
        "UNITTEST_SHUFFLE=1",
        "UNITTEST_RANDOM_SEED=random",
        "UNITTEST_THREADS=1",
        "UNITTEST_XML=1"
        ]
}

def readGitTag() {
    def command = 'git describe --exact-match --tags HEAD'
    def returnStatus = sh(returnStatus: true, script: command)
    if (returnStatus != 0) {
        return null
    }
    return sh(returnStdout: true, script: command).trim()
}

def doPublishLocalArtifacts() {
    return {
        node('docker') {
            deleteDir()
            dir('temp') {
                withAWS(credentials: 'aws-credentials', region: 'us-east-1') {
                    for (publishingStash in publishingStashes) {
                        unstash name: publishingStash
                        def path = publishingStash.replaceAll('___', '/')
                        def files = findFiles(glob: '**')
                        for (file in files) {
                            rlmS3Put file: file.path, path: "downloads/core/${gitDescribeVersion}/${path}/${file.name}"
                            rlmS3Put file: file.path, path: "downloads/core/${file.name}"
                        }
                        deleteDir()
                    }
                }
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
    checkout(
        [
          $class           : 'GitSCM',
          branches         : scm.branches,
          gitTool          : 'native git',
          extensions       : scm.extensions + [[$class: 'CleanCheckout'], [$class: 'CloneOption', depth: 0, noTags: false, reference: '', shallow: false],
                                               [$class: 'SubmoduleOption', disableSubmodules: false, parentCredentials: false, recursiveSubmodules: true,
                                                         reference: '', trackingSubmodules: false]],
          userRemoteConfigs: scm.userRemoteConfigs
        ]
    )
}
