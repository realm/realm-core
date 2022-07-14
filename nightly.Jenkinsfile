
import groovy.json.JsonOutput

def run = build '/realm/realm-core/master'

node('build-notification') 
{
    withCredentials([[$class: 'StringBinding', credentialsId: 'slack-realm-core-ci-alerts-url', variable: 'SLACK_URL']]) {
        def payload = null
        if (currentBuild.getResult() == "SUCCESS") {
            payload = JsonOutput.toJson([
                    username: "Realm CI",
                    icon_emoji: ":realm_new:",
                    text: "*The current realm-core nightly build was ok!*\n<${env.BUILD_URL}|Click here> to check the build."
            ])
        } else if(currentBuild.getResult() == "FAILURE"){
            payload = JsonOutput.toJson([
                    username: "Realm CI",
                    icon_emoji: ":realm_new:",
                    text: "*The current realm-core nightly build is broken!*\n<${env.BUILD_URL}|Click here> to check the build."
            ])
        }
        // otherwise the build was aborted, because no nightly build was needed, but in this case we don't need to signal anything

        if (payload != null) {
            sh "curl -X POST --data-urlencode \'payload=${payload}\' ${env.SLACK_URL}"
        }
    }
}