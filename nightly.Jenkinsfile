
import groovy.json.JsonOutput
def run = build job:'/realm/realm-core/master', propagate: false
node {
    withCredentials([[$class: 'StringBinding', credentialsId: 'slack-realm-core-ci-alerts-url', variable: 'SLACK_URL']]) {
        def payload = null
        if (run.getResult() == "SUCCESS") {
            payload = JsonOutput.toJson([
                    username: "Realm CI",
                    icon_emoji: ":jenkins:",
                    text: "*The current realm-core nightly build was ok!*\n<${run.absoluteUrl}|Click here> to check the build."
            ])
            currentBuild.result = "SUCCESS"
        } else if(run.getResult() == "FAILURE"){
            payload = JsonOutput.toJson([
                    username: "Realm CI",
                    icon_emoji: ":jenkins:",
                    text: "@realm-core-engineers *The current realm-core nightly build is broken!*\n<${run.absoluteUrl}|Click here> to check the build.",
                    link_names: 1
            ])
            currentBuild.result = "FAILURE"
        }
        // otherwise the build was aborted, because no nightly build was needed, but in this case we don't need to signal anything
        if (payload != null) {
            sh "curl -X POST --data-urlencode \'payload=${payload}\' ${env.SLACK_URL}"
        }
    }
}