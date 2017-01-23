#!groovy

@Library('realm-ci') _

node('docker') {
    deleteDir()
    s3Get(bucket: 'static.realm.io', key: 'videos.json')
    s3Put(source: 'videos.json', bucket: 'static.realm.io', key: 'videos.json.bak')
}
