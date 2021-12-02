#!groovy

@Library('realm-ci') _

cocoaStashes = []
androidStashes = []
publishingStashes = []
dependencies = null

tokens = "${env.JOB_NAME}".tokenize('/')
org = tokens[tokens.size()-3]
repo = tokens[tokens.size()-2]
branch = tokens[tokens.size()-1]

ctest_cmd = "ctest -VV"
warningFilters = [
    excludeFile('/external/*'), // submodules and external libraries
    excludeFile('/libuv-src/*'), // libuv, where it was downloaded and built inside cmake
]

jobWrapper {
    stage('gather-info') {
        isPullRequest = !!env.CHANGE_TARGET
        targetBranch = isPullRequest ? env.CHANGE_TARGET : "none"
        rlmNode('docker') {
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
            targetSHA1 = 'NONE'
            if (isPullRequest) {
                targetSHA1 = sh(returnStdout: true, script: "git fetch origin && git merge-base origin/${targetBranch} HEAD").trim()
            }
        }

        currentBranch = env.BRANCH_NAME
        println "Building branch: ${currentBranch}"
        println "Target branch: ${targetBranch}"

        releaseTesting = targetBranch.contains('release')
        isMaster = currentBranch.contains('master')
        longRunningTests = isMaster || currentBranch.contains('next-major')
        isPublishingRun = false
        if (gitTag) {
            isPublishingRun = currentBranch.contains('release')
        }

        echo "Pull request: ${isPullRequest ? 'yes' : 'no'}"
        echo "Release Run: ${releaseTesting ? 'yes' : 'no'}"
        echo "Publishing Run: ${isPublishingRun ? 'yes' : 'no'}"
        echo "Long running test: ${longRunningTests ? 'yes' : 'no'}"

        if (isMaster) {
            // If we're on master, instruct the docker image builds to push to the
            // cache registry
            env.DOCKER_PUSH = "1"
        }
    }

    if (isPullRequest) {
        stage('FormatCheck') {
            rlmNode('docker') {
                getArchive()
                docker.build('realm-core-clang:snapshot', '-f clang.Dockerfile .').inside() {
                    echo "Checking code formatting"
                    modifications = sh(returnStdout: true, script: "git clang-format --diff ${targetSHA1}").trim()
                    try {
                        if (!modifications.equals('no modified files to format')) {
                            if (!modifications.equals('clang-format did not modify any files')) {
                                echo "Commit violates formatting rules"
                                sh "git clang-format --diff ${targetSHA1} > format_error.txt"
                                archiveArtifacts('format_error.txt')
                                sh 'exit 1'
                            }
                        }
                        currentBuild.result = 'SUCCESS'
                    } catch (Exception err) {
                        currentBuild.result = 'FAILURE'
                        throw err
                    }
                }
            }
        }
    }

    stage('Checking') {
        def buildOptions = [
            buildType : 'Debug',
            maxBpNodeSize: 1000,
            enableEncryption: true,
            enableSync: false,
            runTests: true,
        ]
        def linuxOptionsNoEncrypt = [
            buildType : 'Debug',
            maxBpNodeSize: 4,
            enableEncryption: false,
            enableSync: false,
        ]

        parallelExecutors = [
            buildLinuxRelease       : doBuildLinux('Release'),
            checkLinuxDebug         : doCheckInDocker(buildOptions),
            checkLinuxRelease_4     : doCheckInDocker(buildOptions + [maxBpNodeSize: 4, buildType : 'Release']),
            checkLinuxDebug_Sync    : doCheckInDocker(buildOptions + [enableSync: true, dumpChangesetTransform: true]),
            checkLinuxDebugNoEncryp : doCheckInDocker(buildOptions + [enableEncryption: false]),
            checkMacOsRelease_Sync  : doBuildMacOs(buildOptions + [buildType: 'Release', enableSync: true]),
            checkWindows_x86_Release: doBuildWindows('Release', false, 'Win32', true),
            checkWindows_x64_Debug  : doBuildWindows('Debug', false, 'x64', true),
            buildUWP_x86_Release    : doBuildWindows('Release', true, 'Win32', false),
            buildUWP_ARM_Debug      : doBuildWindows('Debug', true, 'ARM', false),
            buildiosDebug           : doBuildAppleDevice('iphoneos', 'Debug'),
            buildAndroidArm64Debug  : doAndroidBuildInDocker('arm64-v8a', 'Debug'),
            buildAndroidTestsArmeabi: doAndroidBuildInDocker('armeabi-v7a', 'Debug', TestAction.Build),
            threadSanitizer         : doCheckSanity(buildOptions + [enableSync: true, sanitizeMode: 'thread']),
            addressSanitizer        : doCheckSanity(buildOptions + [enableSync: true, sanitizeMode: 'address']),
            // FIXME: disabled due to issues with CI
	    // performance             : optionalBuildPerformance(releaseTesting), // always build performance on releases, otherwise make it optional
        ]
        if (releaseTesting) {
            extendedChecks = [
                checkMacOsDebug               : doBuildMacOs(buildOptions + [buildType: "Release"]),
                checkAndroidarmeabiDebug      : doAndroidBuildInDocker('armeabi-v7a', 'Debug', TestAction.Run),
                // FIXME: https://github.com/realm/realm-core/issues/4159
                //checkAndroidx86Release        : doAndroidBuildInDocker('x86', 'Release', TestAction.Run),
                // FIXME: https://github.com/realm/realm-core/issues/4162
                //coverage                      : doBuildCoverage(),
                // valgrind                : doCheckValgrind()
            ]
            parallelExecutors.putAll(extendedChecks)
        }
        parallel parallelExecutors
    }

    if (isPublishingRun) {
        stage('BuildPackages') {
            def buildOptions = [
                enableSync: "ON",
                runTests: false,
            ]

            parallelExecutors = [
                buildMacOsRelease   : doBuildMacOs(buildOptions + [buildType : "Release"]),
                buildCatalystRelease: doBuildMacOsCatalyst('Release'),

                buildLinuxASAN      : doBuildLinuxClang("RelASAN"),
                buildLinuxTSAN      : doBuildLinuxClang("RelTSAN")
            ]

            androidAbis = ['armeabi-v7a', 'x86', 'x86_64', 'arm64-v8a']
            androidBuildTypes = ['Debug', 'Release']

            for (abi in androidAbis) {
                for (buildType in androidBuildTypes) {
                    parallelExecutors["android-${abi}-${buildType}"] = doAndroidBuildInDocker(abi, buildType)
                }
            }

            appleSdks = ['iphoneos', 'iphonesimulator',
                         'appletvos', 'appletvsimulator',
                         'watchos', 'watchsimulator']

            for (sdk in appleSdks) {
                parallelExecutors[sdk] = doBuildAppleDevice(sdk, 'Release')
            }

            linuxBuildTypes = ['Debug', 'Release', 'RelAssert']
            for (buildType in linuxBuildTypes) {
                parallelExecutors["buildLinux${buildType}"] = doBuildLinux(buildType)
            }

            windowsBuildTypes = ['Debug', 'Release']
            windowsPlatforms = ['Win32', 'x64']

            for (buildType in windowsBuildTypes) {
                for (platform in windowsPlatforms) {
                    parallelExecutors["buildWindows-${platform}-${buildType}"] = doBuildWindows(buildType, false, platform, false)
                    parallelExecutors["buildWindowsUniversal-${platform}-${buildType}"] = doBuildWindows(buildType, true, platform, false)
                }
                parallelExecutors["buildWindowsUniversal-ARM-${buildType}"] = doBuildWindows(buildType, true, 'ARM', false)
            }

            parallel parallelExecutors
        }
        stage('Aggregate Cocoa xcframeworks') {
            rlmNode('osx') {
                getArchive()
                for (cocoaStash in cocoaStashes) {
                    unstash name: cocoaStash
                }
                sh 'tools/build-cocoa.sh -x'
                archiveArtifacts('realm-*.tar.*')
                stash includes: 'realm-*.tar.xz', name: "cocoa-xz"
                stash includes: 'realm-*.tar.gz', name: "cocoa-gz"
                publishingStashes << "cocoa-xz"
                publishingStashes << "cocoa-gz"
            }
        }
        stage('Publish to S3') {
            rlmNode('docker') {
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
}

def doCheckInDocker(Map options = [:]) {
    def cmakeOptions = [
        CMAKE_BUILD_TYPE: options.buildType,
        REALM_MAX_BPNODE_SIZE: options.maxBpNodeSize,
        REALM_ENABLE_ENCRYPTION: options.enableEncryption ? 'ON' : 'OFF',
        REALM_ENABLE_SYNC: options.enableSync ? 'ON' : 'OFF',
    ]
    if (options.enableSync) {
        cmakeOptions << [
            REALM_ENABLE_AUTH_TESTS: 'ON',
            REALM_MONGODB_ENDPOINT: 'http://mongodb-realm:9090',
        ]
    }
    if (longRunningTests) {
        cmakeOptions << [
            CMAKE_CXX_FLAGS: '"-DTEST_DURATION=1"',
        ]
    }

    def cmakeDefinitions = cmakeOptions.collect { k,v -> "-D$k=$v" }.join(' ')

    return {
        rlmNode('docker') {
            getArchive()
            def sourcesDir = pwd()
            def buildEnv = docker.build 'realm-core-linux:18.04'
            def environment = environment()
            environment << 'UNITTEST_PROGRESS=1'

            def buildSteps = { String dockerArgs = "" ->
                withEnv(environment) {
                    buildEnv.inside(dockerArgs) {
                        try {
                            dir('build-dir') {
                                sh "cmake ${cmakeDefinitions} -G Ninja .."
                                runAndCollectWarnings(
                                    script: 'ninja',
                                    name: "linux-${options.buildType}-encrypt${options.enableEncryption}-BPNODESIZE_${options.maxBpNodeSize}",
                                    filters: warningFilters,
                                )
                                sh "${ctest_cmd}"
                            }
                        } finally {
                            recordTests("Linux-${options.buildType}")
                        }
                    }
                }
            }

            if (options.enableSync) {
                // stitch images are auto-published every day to our CI
                // see https://github.com/realm/ci/tree/master/realm/docker/mongodb-realm
                // we refrain from using "latest" here to optimise docker pull cost due to a new image being built every day
                // if there's really a new feature you need from the latest stitch, upgrade this manually
                withRealmCloud(version: dependencies.MDBREALM_TEST_SERVER_TAG) { networkName ->
                    buildSteps("--network=${networkName}")
                }

                if (options.dumpChangesetTransform) {
                    buildEnv.inside {
                        dir('build-dir/test') {
                            withEnv([
                                'UNITTEST_PROGRESS=1',
                                'UNITTEST_FILTER=Array_Example Transform_* EmbeddedObjects_*',
                                'UNITTEST_DUMP_TRANSFORM=changeset_dump',
                            ]) {
                                sh '''
                                    ./realm-sync-tests
                                    tar -zcvf changeset_dump.tgz changeset_dump
                                '''
                            }
                            withAWS(credentials: 'stitch-sync-s3', region: 'us-east-1') {
                                retry(20) {
                                    s3Upload file: 'changeset_dump.tgz', bucket: 'realm-test-artifacts', acl: 'PublicRead', path: "sync-transform-corpuses/${gitSha}/"
                                }
                            }
                        }
                    }
                }
            } else {
                buildSteps()
            }
        }
    }
}

def doCheckSanity(Map options = [:]) {
    def privileged = '';

    def cmakeOptions = [
        CMAKE_BUILD_TYPE: options.buildType,
        REALM_MAX_BPNODE_SIZE: options.maxBpNodeSize,
        REALM_ENABLE_SYNC: options.enableSync,
    ]

    if (options.sanitizeMode.contains('thread')) {
        cmakeOptions << [
            REALM_TSAN: "ON",
        ]
    }
    else if (options.sanitizeMode.contains('address')) {
        privileged = '--privileged'
        cmakeOptions << [
            REALM_ASAN: "ON",
        ]
    }

    def cmakeDefinitions = cmakeOptions.collect { k,v -> "-D$k=$v" }.join(' ')

    return {
        rlmNode('docker') {
            getArchive()
            def buildEnv = docker.build('realm-core-linux:clang', '-f clang.Dockerfile .')
            def environment = environment()
            environment << 'UNITTEST_PROGRESS=1'
            withEnv(environment) {
                buildEnv.inside(privileged) {
                    try {
                        dir('build-dir') {
                            sh "cmake ${cmakeDefinitions} -G Ninja .."
                            runAndCollectWarnings(
                                script: 'ninja',
                                parser: "clang",
                                name: "linux-clang-${options.buildType}-${options.sanitizeMode}",
                                filters: warningFilters,
                            )
                            sh "${ctest_cmd}"
                        }

                    } finally {
                        recordTests("Linux-${options.buildType}")
                    }
                }
            }
        }
    }
}

def doBuildLinux(String buildType) {
    return {
        rlmNode('docker') {
            getSourceArchive()

            docker.build('realm-core-generic:gcc-10', '-f generic.Dockerfile .').inside {
                sh """
                   rm -rf build-dir
                   mkdir build-dir
                   cd build-dir
                   scl enable devtoolset-10 -- cmake -DCMAKE_BUILD_TYPE=${buildType} -DREALM_NO_TESTS=1 -G Ninja ..
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

def doBuildLinuxClang(String buildType) {
    return {
        rlmNode('docker') {
            getArchive()
            docker.build('realm-core-linux:clang', '-f clang.Dockerfile .').inside() {
                dir('build-dir') {
                    sh "cmake -D CMAKE_BUILD_TYPE=${buildType} -DREALM_NO_TESTS=1 -G Ninja .."
                    runAndCollectWarnings(
                        script: 'ninja',
                        parser: "clang",
                        name: "linux-clang-${buildType}",
                        filters: warningFilters,
                    )
                    sh 'cpack -G TGZ'
                }
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
        rlmNode('docker') {
            getArchive()
            def buildEnv = docker.build 'realm-core-linux:18.04'
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
                        runAndCollectWarnings(
                            script: 'cd build-dir && ninja',
                            name: "linux-valgrind",
                            filters: warningFilters,
                        )
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

def doAndroidBuildInDocker(String abi, String buildType, TestAction test = TestAction.None) {
    return {
        rlmNode('docker') {
            getArchive()
            def stashName = "android___${abi}___${buildType}"
            def buildDir = "build-${stashName}".replaceAll('___', '-')
            def buildEnv = buildDockerEnv('ci/realm-core:android', extra_args: '-f android.Dockerfile', push: env.BRANCH_NAME == 'master')
            def environment = environment()
            environment << 'UNITTEST_PROGRESS=1'
            def cmakeArgs = ''
            if (test == TestAction.None) {
                cmakeArgs = '-DREALM_NO_TESTS=ON'
            } else if (test.hasValue(TestAction.Build)) {
                // TODO: should we build sync tests, too?
                cmakeArgs = '-DREALM_ENABLE_SYNC=OFF -DREALM_FETCH_MISSING_DEPENDENCIES=ON -DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON'
            }

            def doBuild = {
                buildEnv.inside {
                    runAndCollectWarnings(
                        parser: 'clang',
                        script: "tools/cross_compile.sh -o android -a ${abi} -t ${buildType} -v ${gitDescribeVersion} -f \"${cmakeArgs}\"",
                        name: "android-armeabi-${abi}-${buildType}",
                        filters: warningFilters,
                    )
                }
                if (test == TestAction.None) {
                    dir(buildDir) {
                        archiveArtifacts('realm-*.tar.gz')
                        stash includes: 'realm-*.tar.gz', name: stashName
                    }
                    androidStashes << stashName
                    if (gitTag) {
                        publishingStashes << stashName
                    }
                }
            }

            // if we want to run tests, let's spin up the emulator docker image first
            // it takes a while to warm up so we might as build in the mean time
            // otherwise, just run the build as is
            if (test.hasValue(TestAction.Run)) {
                docker.image('tracer0tong/android-emulator').withRun("-e ARCH=${abi}") { emulator ->
                    doBuild()
                    buildEnv.inside("--link ${emulator.id}:emulator") {
                        try {
                            sh """
                                cd ${buildDir}
                                adb connect emulator
                                timeout 30m adb wait-for-device
                                adb push test/realm-tests /data/local/tmp
                                find test -type f -name "*.json" -maxdepth 1 -exec adb push {} /data/local/tmp \\;
                                find test -type f -name "*.realm" -maxdepth 1 -exec adb push {} /data/local/tmp \\;
                                find test -type f -name "*.txt" -maxdepth 1 -exec adb push {} /data/local/tmp \\;
                                adb shell 'cd /data/local/tmp; ${environment.join(' ')} ./realm-tests || echo __ADB_FAIL__' | tee adb.log
                                ! grep __ADB_FAIL__ adb.log
                            """
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
            } else {
                doBuild()
            }
        }
    }
}

def doBuildWindows(String buildType, boolean isUWP, String platform, boolean runTests) {
    def cpackSystemName = "${isUWP ? 'UWP' : 'Windows'}-${platform}"
    def arch = platform.toLowerCase()
    if (arch == 'win32') {
      arch = 'x86'
    }
    if (arch == 'win64') {
      arch = 'x64'
    }
    def triplet = "${arch}-${isUWP ? 'uwp' : 'windows'}-static"

    def cmakeOptions = [
      CMAKE_GENERATOR_PLATFORM: platform,
      CMAKE_BUILD_TYPE: buildType,
      REALM_ENABLE_SYNC: "ON",
      CPACK_SYSTEM_NAME: cpackSystemName,
      CMAKE_TOOLCHAIN_FILE: "c:\\src\\vcpkg\\scripts\\buildsystems\\vcpkg.cmake",
      VCPKG_TARGET_TRIPLET: triplet,
    ]

     if (isUWP) {
      cmakeOptions << [
        CMAKE_SYSTEM_NAME: 'WindowsStore',
        CMAKE_SYSTEM_VERSION: '10.0',
      ]
    } else {
      cmakeOptions << [
        CMAKE_SYSTEM_VERSION: '8.1',
      ]
    }
    if (!runTests) {
      cmakeOptions << [
        REALM_NO_TESTS: '1',
      ]
    }

    def cmakeDefinitions = cmakeOptions.collect { k,v -> "-D$k=$v" }.join(' ')

    return {
        rlmNode('windows') {
            getArchive()

            dir('build-dir') {
                bat "\"${tool 'cmake'}\" ${cmakeDefinitions} .."
                withEnv(["_MSPDBSRV_ENDPOINT_=${UUID.randomUUID().toString()}"]) {
                    runAndCollectWarnings(
                        parser: 'msbuild',
                        isWindows: true,
                        script: "\"${tool 'cmake'}\" --build . --config ${buildType}",
                        name: "windows-${platform}-${buildType}-${isUWP?'uwp':'nouwp'}",
                        filters: [excludeMessage('Publisher name .* does not match signing certificate subject')] + warningFilters,
                    )
                }
                bat "\"${tool 'cmake'}\\..\\cpack.exe\" -C ${buildType} -D CPACK_GENERATOR=TGZ"
                archiveArtifacts('*.tar.gz')
                if (gitTag) {
                    def stashName = "windows___${platform}___${isUWP?'uwp':'nouwp'}___${buildType}"
                    stash includes:'*.tar.gz', name:stashName
                    publishingStashes << stashName
                }
            }
            if (runTests && !isUWP) {
                def environment = environment() << "TMP=${env.WORKSPACE}\\temp"
                environment << 'UNITTEST_PROGRESS=1'
                withEnv(environment) {
                    dir("build-dir/test/${buildType}") {
                        bat '''
                          mkdir %TMP%
                          realm-tests.exe --no-error-exit-code
                          copy unit-test-report.xml ..\\core-results.xml
                          rmdir /Q /S %TMP%
                        '''
                    }
                }
                if (arch == 'x86') {
                  // On 32-bit Windows we run out of address space when running
                  // the sync tests in parallel
                  environment << 'UNITTEST_THREADS=1'
                }
                withEnv(environment) {
                    dir("build-dir/test/${buildType}") {
                        bat '''
                          mkdir %TMP%
                          realm-sync-tests.exe --no-error-exit-code
                          copy unit-test-report.xml ..\\sync-results.xml
                          rmdir /Q /S %TMP%
                        '''
                    }
                }
                def prefix = "Windows-${platform}-${buildType}";
                recordTests("${prefix}-core", "core-results.xml")
                recordTests("${prefix}-sync", "sync-results.xml")
            }
        }
    }
}

def optionalBuildPerformance(boolean force) {
    if (force) {
        return {
            buildPerformance()
        }
    } else {
        return {
            def doPerformance = true
            stage("Input") {
                try {
                    timeout(time: 10, unit: 'MINUTES') {
                        script {
                            input message: 'Build Performance?', ok: 'Yes'
                        }
                    }
                } catch (err) { // manual abort or timeout
                    println "Not building performance on this run: ${err}"
                    doPerformance = false
                }
            }
            if (doPerformance) {
                stage("Build") {
                    buildPerformance()
                }
            }
        }
    }
}

def buildPerformance() {
    // Select docker-cph-X.  We want docker, metal (brix) and only one executor
    // (exclusive), if the machine changes also change REALM_BENCH_MACHID below
    rlmNode('brix && exclusive') {
      getArchive()

      // REALM_BENCH_DIR tells the gen_bench_hist.sh script where to place results
      // REALM_BENCH_MACHID gives the results an id - results are organized by hardware to prevent mixing cached results with runs on different machines
      // MPLCONFIGDIR gives the python matplotlib library a config directory, otherwise it will try to make one on the user home dir which fails in docker
      docker.build('realm-core-linux:18.04').inside {
        withEnv(["REALM_BENCH_DIR=${env.WORKSPACE}/test/bench/core-benchmarks", "REALM_BENCH_MACHID=docker-brix","MPLCONFIGDIR=${env.WORKSPACE}/test/bench/config"]) {
          rlmS3Get file: 'core-benchmarks.zip', path: 'downloads/core/core-benchmarks.zip'
          sh 'unzip core-benchmarks.zip -d test/bench/'
          sh 'rm core-benchmarks.zip'

          sh """
            cd test/bench
            mkdir -p core-benchmarks results
            ./gen_bench_hist.sh origin/${env.CHANGE_TARGET}
          """
          zip dir: 'test/bench', glob: 'core-benchmarks/**/*', zipFile: 'core-benchmarks.zip'
          rlmS3Put file: 'core-benchmarks.zip', path: 'downloads/core/core-benchmarks.zip'
          sh 'cd test/bench && ./parse_bench_hist.py --local-html results/ core-benchmarks/'
          publishHTML(target: [allowMissing: false, alwaysLinkToLastBuild: false, keepAll: true, reportDir: 'test/bench/results', reportFiles: 'report.html', reportName: 'Performance_Report'])
          withCredentials([[$class: 'StringBinding', credentialsId: 'bot-github-token', variable: 'githubToken']]) {
              sh "curl -H \"Authorization: token ${env.githubToken}\" " +
                 "-d '{ \"body\": \"Check the performance result [here](${env.BUILD_URL}Performance_5fReport).\"}' " +
                 "\"https://api.github.com/repos/realm/${repo}/issues/${env.CHANGE_ID}/comments\""
          }
        }
      }
    }
}

def doBuildMacOs(Map options = [:]) {
    def buildType = options.buildType;
    def sdk = 'macosx'

    def cmakeOptions = [
        CMAKE_BUILD_TYPE: options.buildType,
        CMAKE_TOOLCHAIN_FILE: "../tools/cmake/macosx.toolchain.cmake",
        REALM_ENABLE_SYNC: options.enableSync,
    ]
    if (!options.runTests) {
        cmakeOptions << [
            REALM_NO_TESTS: "ON",
        ]
    }
    if (longRunningTests) {
        cmakeOptions << [
            CMAKE_CXX_FLAGS: '"-DTEST_DURATION=1"',
        ]
    }

    def cmakeDefinitions = cmakeOptions.collect { k,v -> "-D$k=$v" }.join(' ')

    return {
        rlmNode('osx') {
            getArchive()

            dir("build-macosx-${buildType}") {
                withEnv(['DEVELOPER_DIR=/Applications/Xcode-12.2.app/Contents/Developer/']) {
                    // This is a dirty trick to work around a bug in xcode
                    // It will hang if launched on the same project (cmake trying the compiler out)
                    // in parallel.
                    retry(3) {
                        timeout(time: 2, unit: 'MINUTES') {
                            sh """
                                rm -rf *
                                cmake ${cmakeDefinitions} -D REALM_VERSION=${gitDescribeVersion} -G Ninja ..
                            """
                        }
                    }

                    runAndCollectWarnings(
                        parser: 'clang',
                        script: 'ninja package',
                        name: "osx-clang-${buildType}",
                        filters: warningFilters,
                    )
                }
            }
            withEnv(['DEVELOPER_DIR=/Applications/Xcode-12.2.app/Contents/Developer']) {
                runAndCollectWarnings(
                    parser: 'clang',
                    script: 'xcrun swift build',
                    name: "osx-clang-xcrun-swift-${buildType}",
                    filters: warningFilters,
                )
                sh 'xcrun swift run ObjectStoreTests'
            }

            archiveArtifacts("build-macosx-${buildType}/*.tar.gz")

            def stashName = "macosx___${buildType}"
            stash includes:"build-macosx-${buildType}/*.tar.gz", name:stashName
            cocoaStashes << stashName
            publishingStashes << stashName

            if (options.runTests) {
                try {
                    def environment = environment()
                    environment << 'UNITTEST_PROGRESS=1'
                    environment << 'CTEST_OUTPUT_ON_FAILURE=1'
                    dir("build-macosx-${buildType}") {
                        withEnv(environment) {
                            sh "${ctest_cmd}"
                        }
                    }
                } finally {
                    // recordTests expects the test results xml file in a build-dir/test/ folder
                    sh """
                        mkdir -p build-dir/test
                        cp build-macosx-${buildType}/test/unit-test-report.xml build-dir/test/
                    """
                    recordTests("macosx_${buildType}")
                }
            }
        }
    }
}

def doBuildMacOsCatalyst(String buildType) {
    return {
        rlmNode('osx') {
            getArchive()

            dir("build-maccatalyst-${buildType}") {
                withEnv(['DEVELOPER_DIR=/Applications/Xcode-12.2.app/Contents/Developer/']) {
                    sh """
                            rm -rf *
                            cmake -D CMAKE_TOOLCHAIN_FILE=../tools/cmake/maccatalyst.toolchain.cmake \\
                                  -D CMAKE_BUILD_TYPE=${buildType} \\
                                  -D REALM_VERSION=${gitDescribeVersion} \\
                                  -D REALM_SKIP_SHARED_LIB=ON \\
                                  -D REALM_BUILD_LIB_ONLY=ON \\
                                  -G Ninja ..
                        """
                    runAndCollectWarnings(
                        parser: 'clang',
                        script: 'ninja package',
                        name: "osx-maccatalyst-${buildType}",
                        filters: warningFilters,
                    )
                }
            }

            archiveArtifacts("build-maccatalyst-${buildType}/*.tar.gz")

            def stashName = "maccatalyst__${buildType}"
            stash includes:"build-maccatalyst-${buildType}/*.tar.gz", name:stashName
            cocoaStashes << stashName
            publishingStashes << stashName
        }
    }
}

def doBuildAppleDevice(String sdk, String buildType) {
    return {
        rlmNode('osx') {
            getArchive()

            withEnv(["DEVELOPER_DIR=/Applications/Xcode-12.2.app/Contents/Developer/"]) {
                retry(3) {
                    timeout(time: 45, unit: 'MINUTES') {
                        sh """
                            rm -rf build-*
                            tools/cross_compile.sh -o ${sdk} -t ${buildType} -v ${gitDescribeVersion}
                        """
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
    rlmNode('docker') {
      getArchive()
      docker.build('realm-core-linux:18.04').inside {
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
def recordTests(tag, String reportName = "unit-test-report.xml") {
    def tests = readFile("build-dir/test/${reportName}")
    def modifiedTests = tests.replaceAll('realm-core-tests', tag)
    writeFile file: 'build-dir/test/modified-test-report.xml', text: modifiedTests
    junit testResults: 'build-dir/test/modified-test-report.xml'
}

def environment() {
    return [
        "UNITTEST_SHUFFLE=1",
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

enum TestAction {
    None(0x0),
    Build(0x1),
    Run(0x3); // build and run

    private final long value;

    TestAction(long value) {
        this.value = value;
    }

    public boolean hasValue(TestAction value) {
        return (this.value & value.value) == value.value;
    }
}
