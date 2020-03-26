@Library('jenkins-helpers') _
testBuildReleasePoetryPackage {
    releaseToPypi = true
    releaseToArtifactory = true
    uploadCoverageReport = true
    topLevelPackageName = 'cognite'
    testWithTox = true
    toxEnvList = ['py36', 'py37', 'py38']
}
