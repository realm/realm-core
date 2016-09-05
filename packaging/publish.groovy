#!groovy

@NonCPS
def parseJson(json) {
  def slurper = new groovy.json.JsonSlurperClassic()
  def ret = slurper.parseText(json)
  slurper = null
  return ret
}

def curl(verb, baseUrl, httpAuth, path, extraParams) {
  sh """
    curl -sX '${verb}' 'https://${httpAuth}@${baseUrl}/${path}' -o 'output' ${extraParams}
  """
  def output = readFile('output')
  sh 'rm -rf output'
  return parseJson(output)
}

def packageCloudAPI(verb, path, extraParams = '') {
  withCredentials([[
    $class: 'UsernamePasswordMultiBinding',
    credentialsId: '14f8156d-c57c-4968-b7ea-f67bef26d812',
    passwordVariable: 'api_token',
    usernameVariable: 'api_username']])
  {
    def ret = curl(verb, 'packagecloud.io', "${env.api_token}:", path, extraParams)
    return ret
  }
}

def getPackageCloudDistributionID(fileType, distroName, distroVersion) {
  for (distribution in packageCloudAPI('GET', '/api/v1/distributions')[fileType]) {
    if (distribution['index_name'] == distroName) {
      for (version in distribution['versions']) {
        if (version['index_name'] == "${distroVersion}") {
          return version['id']
        }
      }
    }
  }
}

def pushToPackageCloud(repoName, fileName, distroId) {
  def extraParams = "-F 'package[distro_version_id]=${distroId}' -F 'package[package_file]=@${fileName}'"
  def ret = packageCloudAPI('POST', "/api/v1/repos/realm/${repoName}/packages.json", extraParams)
  echo "${ret}"
}

def getFileNames(pattern) {
  sh "find . -name '${pattern}' > output"
  def output = readFile('output')
  sh 'rm -rf output'
  return output.split('\n')
}

def uploadPackages(repoName, fileType, distroName, distroVersion, pattern) {
  def distroId = getPackageCloudDistributionID(fileType, distroName, distroVersion)
  def fileNames = getFileNames(pattern)
  for (def i = 0; i < fileNames.size(); ++i) {
    pushToPackageCloud(repoName, fileNames[i], distroId)
  }
}

return this;