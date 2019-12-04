@Library('jenkins-helpers@v0.1.34') _

def label = "python-extractor-utils-${UUID.randomUUID().toString()}"

podTemplate(
    label: label,
    containers: [
        containerTemplate(
            name: 'python',
            image: 'eu.gcr.io/cognitedata/multi-python:7040fac',
            command: '/bin/cat -',
            resourceRequestCpu: '1000m',
            resourceRequestMemory: '500Mi',
            resourceLimitCpu: '1000m',
            resourceLimitMemory: '500Mi',
            envVars: [
                secretEnvVar(key: 'COGNITE_API_KEY', secretName: 'extractor-tests', secretKey: 'extractor-tests'),
                secretEnvVar(key: 'CODECOV_TOKEN', secretName: 'codecov-tokens', secretKey: 'python-extractor-utils'),
                envVar(key: 'COGNITE_PROJECT', value: 'extractor-tests'),
                envVar(key: 'COGNITE_BASE_URL', value: 'https://greenfield.cognitedata.com'),
                envVar(key: 'JENKINS_URL', value: env.JENKINS_URL),
                envVar(key: 'BRANCH_NAME', value: env.BRANCH_NAME),
                envVar(key: 'BUILD_NUMBER', value: env.BUILD_NUMBER),
                envVar(key: 'BUILD_URL', value: env.BUILD_URL),
                envVar(key: 'CHANGE_ID', value: env.CHANGE_ID),
                envVar(key: 'PYTHONPATH', value: '/usr/local/bin:$pwd+/../'),
            ],
            ttyEnabled: true
        ),
    ],
    volumes: [
        secretVolume(secretName: 'pypi-credentials', mountPath: '/pypi', readOnly: true),
        configMapVolume(configMapName: 'codecov-script-configmap', mountPath: '/codecov-script'),
    ]
) {
    node(label) {
        def pipVersion
        def currentVersion

        container('jnlp') {
            stage('Checkout') {
                checkout(scm)
            }
        }
        container('python') {
            stage('Install pipenv') {
                sh("pip3 install pipenv")
            }
            stage('Install dependencies') {
                sh("pipenv sync --dev")
                sh("pipenv run pip list")
                sh("pipenv graph")
            }
            stage('Check code style') {
                sh("pipenv run black -l 120 --check .")
                sh("pipenv run isort -w 120 -m 3 -tc -rc --check-only .")
            }
            stage('Test') {
                sh("pipenv run python -m pytest -v --cov-report xml:coverage.xml --cov=cognite/extractorutils --junitxml=test-report.xml")
                junit(allowEmptyResults: true, testResults: '**/test-report.xml')
                summarizeTestResults()
                sh 'bash </codecov-script/upload-report.sh'
                step([$class: 'CoberturaPublisher', coberturaReportFile: 'coverage.xml'])
            }
            stage('Build') {
                sh("python3 setup.py sdist bdist_wheel")
            }
            stage('Build Docs') {
                dir('./docs'){
                    sh("pipenv run sphinx-build -W -b html ./source ./build")
                }
            }
            stage('Get latest version on PyPI') {
                pipVersion = sh(returnStdout: true, script: 'pipenv run yolk -V cognite-extractor-utils | sort -n | tail -1 | cut -d\\  -f 2').trim()
                currentVersion = sh(returnStdout: true, script: 'sed -n -e "/^__version__/p" cognite/extractorutils/__init__.py | cut -d\\" -f2').trim()
                println("This version: " + currentVersion)
                println("Latest pip version: " + pipVersion)
            }
            if (env.BRANCH_NAME == 'master' && currentVersion != pipVersion) {
             stage('Release') {
                 sh("pipenv run twine upload --config-file /pypi/.pypirc dist/*")
             }
         }
     }
 }
}
