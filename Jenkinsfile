#!groovy

@Library('realm-ci') _

node('docker') {
    s3Download(bucket: 'static.realm.io', key: 'videos.json')
}
