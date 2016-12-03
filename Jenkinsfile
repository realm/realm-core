#!groovy
def getSourceArchive() {
  checkout scm
  sh 'git clean -ffdx -e .????????'
  sshagent(['realm-ci-ssh']) {
    sh 'git submodule update --init --recursive'
  }
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

def buildDockerEnv(name, dockerfile='Dockerfile', extra_args='') {
  docker.withRegistry("https://${env.DOCKER_REGISTRY}", "ecr:eu-west-1:aws-ci-user") {
    sh "sh ./workflow/docker_build_wrapper.sh $name . ${extra_args}"
  }
  return docker.image(name)
}

def publishReport(String label) {
  // Unfortunately, we cannot add a title or tag to individual coverage reports.
  echo "Unstashing coverage-${label}"
  unstash("coverage-${label}")
  step([
    $class: 'CoberturaPublisher',
    autoUpdateHealth: false,
    autoUpdateStability: false,
    coberturaReportFile: "${label}.build/coverage.xml",
    failNoReports: true,
    failUnhealthy: false,
    failUnstable: false,
    maxNumberOfBuilds: 0,
    onlyStable: false,
    sourceEncoding: 'ASCII',
    zoomCoverageChart: false
  ])
}

if (env.BRANCH_NAME == 'master') {
  env.DOCKER_PUSH = "1"
}

def doDockerBuild(String flavor, Boolean withCoverage, Boolean enableSync) {
  def sync = enableSync ? "sync" : ""
  def label = "${flavor}${enableSync ? '-sync' : ''}"
  
  return {
    node('docker') {
      getSourceArchive()
      def image = buildDockerEnv("ci/realm-object-store:${flavor}")
      sshagent(['realm-ci-ssh']) {
        image.inside("-v /etc/passwd:/etc/passwd:ro -v ${env.HOME}:${env.HOME} -v ${env.SSH_AUTH_SOCK}:${env.SSH_AUTH_SOCK} -e HOME=${env.HOME}") {
          if(withCoverage) {
            sh "./workflow/test_coverage.sh ${sync} && mv coverage.build ${label}.build"
          } else {
            sh "./workflow/build.sh ${flavor} ${sync}"
          }
        }
      }
      if(withCoverage) {
        echo "Stashing coverage-${label}"
        stash includes: "${label}.build/coverage.xml", name: "coverage-${label}"
      }
    }
  }
}

def doBuild(String nodeSpec, String flavor, Boolean enableSync) {
  def sync = enableSync ? "sync" : ""
  def label = "${flavor}${enableSync ? '-sync' : ''}"
  return {
    node(nodeSpec) {
      getSourceArchive()
      sshagent(['realm-ci-ssh']) {
        sh "./workflow/test_coverage.sh ${sync} && mv coverage.build ${label}.build"
      }
      echo "Stashing coverage-${label}"
      stash includes: "${label}.build/coverage.xml", name: "coverage-${label}"
    }
  }
}

def setBuildName(newBuildName) {
  currentBuild.displayName = "${currentBuild.displayName} - ${newBuildName}"
}

stage('prepare') {
  node('docker') {
    getSourceArchive()

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
    linux: doDockerBuild('linux', true, false),
    linux_sync: doDockerBuild('linux', true, true),
    android: doDockerBuild('android', false, false),
    macos: doBuild('osx', 'macOS', false),
    macos_sync: doBuild('osx', 'macOS', true)
  )
  currentBuild.result = 'SUCCESS'
}

stage('publish') {
  node('docker') {
    publishReport('linux')
    publishReport('linux-sync')
    publishReport('macOS')
    publishReport('macOS-sync')
  }
}
