@Library('jenkins-helpers') _
testBuildReleasePoetryPackage {
    releaseToPypi = true
    releaseToArtifactory = true
    uploadCoverageReport = true
    topLevelPackageName = 'cognite'
    testWithTox = true
    resourceRequestCpu = '1000m'
    toxEnvList = ['py36', 'py37', 'py38']
    extraEnvVars = [
        secretEnvVar(key: 'CODECOV_TOKEN', secretName: 'codecov-tokens', secretKey: 'python-extractor-utils'),
        secretEnvVar(key: 'COGNITE_API_KEY', secretName: 'extractor-tests', secretKey: 'extractor-tests'),
        envVar(key: 'COGNITE_BASE_URL', value: "https://greenfield.cognitedata.com"),
        envVar(key: 'COGNITE_PROJECT', value: "extractor-tests"),
    ]
}
