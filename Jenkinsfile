// vim: set filetype=groovy:

@Library(["camunda-ci", "zeebe-jenkins-shared-library"]) _

// the build name will be used as a kubernetes label, and kubernetes has strict syntax rules - see
// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
def buildName = "${env.JOB_BASE_NAME.replaceAll("%2F", "-").replaceAll("\\.", "-").take(20)}-${env.BUILD_ID}"

def mainBranchName = 'main'
def isMainBranch = env.BRANCH_NAME == mainBranchName
def latestStableBranchName = 'stable/1.3'
def isLatestStable = env.BRANCH_NAME == latestStableBranchName

// for the main branch keep builds for 7 days to be able to analyse build errors, for all other branches, keep the last 10 builds
def daysToKeep = isMainBranch ? '7' : '-1'
def numToKeep = isMainBranch ? '-1' : '10'

// single step timeouts - remember to be generous to avoid occasional slow downs, e.g. waiting to be
// scheduled by Kubernetes, slow downloads of remote docker images, etc.
def shortTimeoutMinutes = 10
def longTimeoutMinutes = 45

// the IT agent needs to share some files for post analysis, and since they share the same name as
// those in the main agent, we unstash them in a separate directory
def itAgentUnstashDirectory = '.tmp/it'
def itFlakyTestStashName = 'it-flakyTests'

// the main branch should be run at midnight to do a nightly build including QA test run
// the latest stable branch is run two hour later at 01:00 AM.
def cronTrigger = isMainBranch ? '0 0 * * *' : isLatestStable ? '0 2 * * *' : ''

// since we report the build status to CI analytics at the very end, when the build is finished, we
// need to share the result of the flaky test analysis between different stages, so using a global
// variable is a necessary evil here
def flakyTestCases = []

pipeline {
    agent {
        kubernetes {
            cloud 'zeebe-ci'
            label "zeebe-ci-build_${buildName}"
            defaultContainer 'jnlp'
            yaml templatePodspec('.ci/podSpecs/distribution-template.yml')
        }
    }

    environment {
        NEXUS = credentials("camunda-nexus")
        SONARCLOUD_TOKEN = credentials('zeebe-sonarcloud-token')
    }

    triggers {
        cron(cronTrigger)
    }

    options {
        buildDiscarder(logRotator(daysToKeepStr: daysToKeep, numToKeepStr: numToKeep))
        timestamps()
    }

    parameters {
        booleanParam(name: 'SKIP_VERIFY', defaultValue: false, description: "Skip 'Verify' Stage")
        booleanParam(name: 'RUN_QA', defaultValue: false, description: "Run QA Stage")
        string(name: 'GENERATION_TEMPLATE', defaultValue: 'Zeebe SNAPSHOT', description: "Generation template for QA tests (the QA test will be run with this Zeebe version and Operate/Elasticsearch version from the generation template)")
    }

    stages {
        stage('Prepare Distribution') {
            steps {
                timeout(time: shortTimeoutMinutes, unit: 'MINUTES') {
                    setHumanReadableBuildDisplayName()

                    prepareMavenContainer()
                    prepareMavenContainer('jdk8')
                    container('golang') {
                        sh '.ci/scripts/distribution/prepare-go.sh'
                    }

                    runMavenContainerCommand(".ci/scripts/distribution/ensure-naming-for-process.sh")

                    // prepare unstash directory for IT files - required since the file names will
                    // be the same as in the other stages. it's necessary to set the permissions to
                    // 0777 has shell scripts are executed as root, whereas Jenkins directives such
                    // as unstash are executed as jenkins
                    runMavenContainerCommand("mkdir -m 0777 -p ${itAgentUnstashDirectory}")
                }
            }
        }

        stage('Build Distribution') {
            environment {
                VERSION = readMavenPom(file: 'bom/pom.xml').getVersion()
            }
            steps {
                timeout(time: shortTimeoutMinutes, unit: 'MINUTES') {
                    // since zbctl is included in camunda-zeebe.tar.gz, which is produced by
                    // maven, we have to build the go artifacts first
                    container('golang') {
                        sh '.ci/scripts/distribution/build-go.sh'
                    }
                    runMavenContainerCommand('.ci/scripts/distribution/build-java.sh')

                    // to simplify building the Docker image, we copy the distribution to a fixed
                    // filename that doesn't include the version
                    runMavenContainerCommand('cp dist/target/camunda-zeebe-*.tar.gz camunda-zeebe.tar.gz')

                    container('python') {
                        camundaGCloudSaveTmpFile('zeebe-distro', ['camunda-zeebe.tar.gz'])

                        sh "tar -cf zeebe-build.tar ./m2-repository/io/camunda/*/${VERSION}/*"
                        camundaGCloudSaveTmpFile('zeebe-build', ['zeebe-build.tar'])
                    }
                }
            }
        }

        stage('Build Docker Images') {
            environment {
                DOCKER_BUILDKIT = "1"
                IMAGE = "camunda/zeebe"
                TAG = 'current-test'
            }

            steps {
                timeout(time: shortTimeoutMinutes, unit: 'MINUTES') {
                    container('docker') {
                        sh '.ci/scripts/docker/build.sh'
                    }
                }
            }
        }

        stage('Verify') {
            when { not { expression { params.SKIP_VERIFY } } }
            parallel {
                stage('Test (Go)') {
                    steps {
                        timeout(time: longTimeoutMinutes, unit: 'MINUTES') {
                            container('golang') {
                                sh '.ci/scripts/distribution/test-go.sh'
                            }
                        }
                    }

                    post {
                        always {
                            junit testResults: "**/*/TEST-go.xml", keepLongStdio: true, allowEmptyResults: true
                        }
                    }
                }

                stage('Test') {
                    environment {
                        SUREFIRE_REPORT_NAME_SUFFIX = 'java-testrun'
                        MAVEN_PARALLELISM = 2
                        SUREFIRE_FORK_COUNT = 6
                        JUNIT_THREAD_COUNT = 6
                    }

                    stages {
                        stage('Test (Java)') {
                            steps {
                                timeout(time: longTimeoutMinutes, unit: 'MINUTES') {
                                    runMavenContainerCommand('.ci/scripts/distribution/test-java.sh')
                                }
                            }
                        }
                        stage('Analyse') {
                            steps {
                                timeout(time: longTimeoutMinutes, unit: 'MINUTES') {
                                    runMavenContainerCommand('.ci/scripts/distribution/analyse-java.sh')
                                }
                            }
                        }
                    }

                    post {
                        always {
                            junit testResults: "**/*/TEST*${SUREFIRE_REPORT_NAME_SUFFIX}.xml", keepLongStdio: true, allowEmptyResults: true
                            junit testResults: "**/*/TEST*${SUREFIRE_REPORT_NAME_SUFFIX}-FLAKY.xml", keepLongStdio: true, allowEmptyResults: true
                        }
                        failure {
                            archiveArtifacts artifacts: '**/FlakyTests.txt, **/DuplicateTests.txt', allowEmptyArchive: true
                        }
                    }
                }

                stage('Test (Random)') {
                    environment {
                        SUREFIRE_REPORT_NAME_SUFFIX = 'random-testrun'
                        MAVEN_PARALLELISM = 2
                        SUREFIRE_FORK_COUNT = 6
                        JUNIT_THREAD_COUNT = 6
                    }

                    steps {
                        timeout(time: longTimeoutMinutes, unit: 'MINUTES') {
                            runMavenContainerCommand('.ci/scripts/distribution/random-test-java.sh')
                        }
                    }

                    post {
                        always {
                            junit testResults: "**/*/TEST*${SUREFIRE_REPORT_NAME_SUFFIX}.xml", keepLongStdio: true, allowEmptyResults: true
                            junit testResults: "**/*/TEST*${SUREFIRE_REPORT_NAME_SUFFIX}-FLAKY.xml", keepLongStdio: true, allowEmptyResults: true
                        }
                        failure {
                            archiveArtifacts artifacts: '**/DuplicateTests.txt', allowEmptyArchive: true
                        }
                    }
                }

                stage('Test (Java 8)') {
                    environment {
                        SUREFIRE_REPORT_NAME_SUFFIX = 'java8-testrun'
                    }

                    steps {
                        timeout(time: longTimeoutMinutes, unit: 'MINUTES') {
                            runMavenContainerCommand('.ci/scripts/distribution/test-java8.sh', 'jdk8')
                        }
                    }

                    post {
                        always {
                            junit testResults: "**/*/TEST*${SUREFIRE_REPORT_NAME_SUFFIX}.xml", keepLongStdio: true, allowEmptyResults: true
                            junit testResults: "**/*/TEST*${SUREFIRE_REPORT_NAME_SUFFIX}-FLAKY.xml", keepLongStdio: true, allowEmptyResults: true
                        }
                        failure {
                            archiveArtifacts artifacts: '**/FlakyTests.txt, **/DuplicateTests.txt', allowEmptyArchive: true
                        }
                    }
                }

                stage('IT') {
                    // NOTE: all nested stages in the IT stage will be run on the following agent,
                    // identified by its label. Keep in mind that any artefacts produced in this
                    // agent will be unavailable in other agents, so you will need to copy them
                    // NOTE: agents (except the main one) are terminated once their stage is
                    // finished, so if you want to run several steps/stages in that agent they have
                    // to be sequential (since you cannot nest parallels/matrixes). if you need
                    // parallelism, consider spawning a sub-job
                    agent {
                        kubernetes {
                            cloud 'zeebe-ci'
                            label "zeebe-ci-build_${buildName}_it"
                            defaultContainer 'jnlp'
                            yaml templatePodspec('.ci/podSpecs/integration-test-template.yml')
                        }
                    }

                    stages {
                        stage('Prepare') {
                            environment {
                                DOCKER_BUILDKIT = 1
                                IMAGE = "camunda/zeebe"
                                TAG = "current-test"
                            }

                            steps {
                                timeout(time: shortTimeoutMinutes, unit: 'MINUTES') {
                                    prepareMavenContainer()

                                    container('python') {
                                        camundaGCloudRestoreTmpFile('zeebe-build', ['zeebe-build.tar'])
                                        camundaGCloudRestoreTmpFile('zeebe-distro', ['camunda-zeebe.tar.gz'])
                                        sh "tar -xf zeebe-build.tar"
                                    }

                                    container('docker') {
                                        sh '.ci/scripts/docker/local-registry.sh'
                                    }

                                    withVault(
                                        [vaultSecrets: [
                                            [path: 'secret/products/zeebe/ci/jenkins', secretValues: [
                                                [envVar: 'TC_CLOUD_TOKEN', vaultKey: 'TC_CLOUD_TOKEN'],
                                            ]],
                                    ]]) {
                                        runMavenContainerCommand('.ci/scripts/distribution/it-prepare.sh')
                                    }
                                }
                            }
                        }

                        stage('Test') {
                            environment {
                                SUREFIRE_REPORT_NAME_SUFFIX = 'it-testrun'
                                MAVEN_PARALLELISM = 2
                                SUREFIRE_FORK_COUNT = 12
                                JUNIT_THREAD_COUNT = 12
                                ZEEBE_TEST_DOCKER_IMAGE = "localhost:5000/camunda/zeebe:current-test"
                            }

                            steps {
                                timeout(time: longTimeoutMinutes, unit: 'MINUTES') {
                                    runMavenContainerCommand('.ci/scripts/distribution/it-java.sh')
                                }
                            }

                            post {
                                always {
                                    junit testResults: "**/*/TEST*${SUREFIRE_REPORT_NAME_SUFFIX}.xml", keepLongStdio: true, allowEmptyResults: true
                                    junit testResults: "**/*/TEST*${SUREFIRE_REPORT_NAME_SUFFIX}-FLAKY.xml", keepLongStdio: true, allowEmptyResults: true
                                    stash allowEmpty: true, name: itFlakyTestStashName, includes: '**/FlakyTests.txt'
                                }

                                failure {
                                    zip zipFile: 'test-reports-it.zip', archive: true, glob: "**/*/failsafe-reports/**"
                                    zip zipFile: 'test-errors-it.zip', archive: true, glob: "**/hs_err_*.log"
                                    archiveArtifacts artifacts: '**/FlakyTests.txt, **/DuplicateTests.txt', allowEmptyArchive: true
                                }
                            }
                        }
                    }
                }
            }

            post {
                always {
                    checkCodeCoverage()
                }

                failure {
                    zip zipFile: 'test-reports.zip', archive: true, glob: "**/*/surefire-reports/**"
                    zip zipFile: 'test-errors.zip', archive: true, glob: "**/hs_err_*.log"
                    dir(itAgentUnstashDirectory) {
                        unstash name: itFlakyTestStashName
                    }

                    script {
                        def flakeFiles = ['./FlakyTests.txt', "${itAgentUnstashDirectory}/FlakyTests.txt"]
                        def flakes = combineFlakeResults(flakeFiles)

                        flakyTestCases = [flakes].flatten()
                        if (flakes) {
                            currentBuild.description = "Flaky tests (#${flakyTestCases.size()}): [<br />${flakyTestCases.join(',<br />')}]"
                        }
                    }
                }
            }
        }

        stage('QA') {
            when {
                anyOf {
                    expression { params.RUN_QA }
                    allOf {
                        anyOf {
                            branch mainBranchName
                            branch latestStableBranchName
                        }
                        triggeredBy 'TimerTrigger'
                    }
                }
            }
            environment {
                IMAGE = "gcr.io/zeebe-io/zeebe"
                VERSION = readMavenPom(file: 'bom/pom.xml').getVersion()
                TAG = "${env.VERSION}-${env.GIT_COMMIT}"
                DOCKER_GCR = credentials("zeebe-gcr-serviceaccount-json")
                ZEEBE_AUTHORIZATION_SERVER_URL = 'https://login.cloud.ultrawombat.com/oauth/token'
                ZEEBE_CLIENT_ID = 'S7GNoVCE6J-8L~OdFiI59kWM19P.wvKo'
                QA_RUN_VARIABLES = "{\"zeebeImage\": \"${env.IMAGE}:${env.TAG}\", \"generationTemplate\": \"${params.GENERATION_TEMPLATE}\", " +
                                    "\"channel\": \"Internal Dev\", \"branch\": \"${env.BRANCH_NAME}\", \"build\": \"${currentBuild.absoluteUrl}\", " +
                                    "\"businessKey\": \"${currentBuild.absoluteUrl}\", \"processId\": \"qa-protocol\"}"
                BUSINESS_KEY = "${currentBuild.absoluteUrl}"
            }

            steps {
                container('docker') {
                    sh '.ci/scripts/docker/upload-gcr.sh'
                }
                container('maven') {
                    withVault(
                        [vaultSecrets:
                             [
                                 [path        : 'secret/products/zeebe/ci/testbench-secrets-1.x-prod',
                                  secretValues:
                                      [
                                          [envVar: 'ZEEBE_CLIENT_SECRET', vaultKey: 'clientSecret'],
                                          [envVar: 'ZEEBE_ADDRESS', vaultKey: 'contactPoint'],
                                      ]
                                 ],
                             ]
                        ]
                    ) {
                        sh '.ci/scripts/distribution/qa-testbench.sh'
                    }
                }
            }

            post {
                failure {
                    script {
                        currentBuild.description = 'Failure in QA Stage'
                    }
                }
            }
        }
    }

    post {
        always {
            // Retrigger the build if there were connection issues (but not
            // on bors staging branch as bors does not recognize this result)
            script {
                if (agentDisconnected()) {
                    currentBuild.result = 'ABORTED'
                    currentBuild.description = "Aborted due to connection error"

                    if (!isBorsStagingBranch()) {
                        build job: currentBuild.projectName, propagate: false, quietPeriod: 60, wait: false
                    }
                }

                // we track each flaky test as a separate result so we can count how "flaky" a test is
                if (flakyTestCases) {
                    for (flakyTestCase in flakyTestCases) {
                        org.camunda.helper.CIAnalytics.trackBuildStatus(this, 'flaky-tests', flakyTestCase)
                    }
                } else {
                    org.camunda.helper.CIAnalytics.trackBuildStatus(this, currentBuild.result)
                }
            }
        }

        failure {
            script {
                if (env.BRANCH_NAME != mainBranchName || agentDisconnected()) {
                    return
                }
                sendZeebeSlackMessage()
            }
        }

        changed {
            script {
                if (env.BRANCH_NAME != mainBranchName || agentDisconnected()) {
                    return
                }
                if (currentBuild.currentResult == 'FAILURE') {
                    return // already handled above
                }
                if (!hasBuildResultChanged()) {
                    return
                }

                sendZeebeSlackMessage()
            }
        }
    }
}

//////////////////// Helper functions ////////////////////

def getMavenContainerNameForJDK(String jdk = null) {
    "maven${jdk ? '-' + jdk : ''}"
}

def prepareMavenContainer(String jdk = null) {
    container(getMavenContainerNameForJDK(jdk)) {
        sh '.ci/scripts/distribution/prepare.sh'
    }
}

def runMavenContainerCommand(String shellCommand, String jdk = null) {
    container(getMavenContainerNameForJDK(jdk)) {
        configFileProvider([configFile(fileId: 'maven-nexus-settings-zeebe-local-repo', variable: 'MAVEN_SETTINGS_XML')]) {
            sh shellCommand
        }
    }
}

// TODO: can be extracted to zeebe-jenkins-shared-library
def setHumanReadableBuildDisplayName(int maximumLength = 45) {
    script {
        commit_summary = sh([returnStdout: true, script: 'git show -s --format=%s']).trim()
        displayNameFull = "#${env.BUILD_NUMBER}: ${commit_summary}"

        if (displayNameFull.length() <= maximumLength) {
            currentBuild.displayName = displayNameFull
        } else {
            displayStringHardTruncate = displayNameFull.take(maximumLength)
            currentBuild.displayName = displayStringHardTruncate.take(displayStringHardTruncate.lastIndexOf(' '))
        }
    }
}

// TODO: can be extracted to zeebe-jenkins-shared-library
def sendZeebeSlackMessage() {
    echo "Send slack message"
    slackSend(
        channel: "#zeebe-ci${jenkins.model.JenkinsLocationConfiguration.get()?.getUrl()?.contains('stage') ? '-stage' : ''}",
        message: "Zeebe ${env.BRANCH_NAME} build ${currentBuild.absoluteUrl} changed status to ${currentBuild.currentResult}")
}

def isBorsStagingBranch() {
    env.BRANCH_NAME == 'staging'
}

def templatePodspec(String podspecPath, flags = [:]) {
    def defaultFlags = [
        /* Criteria for using stable node pools:
         * - staging branch: to have smooth staging builds and avoid unnecessary retries
         * - params.RUN_QA: during QA stage the node must wait for the result. This can take several hours. Therefore a stable node is needed
         * - env.isMainBranch: the core requirement is to have a stable node for nightly builds, which also rn QA (see above)
         */
        useStableNodePool: isBorsStagingBranch() || params.RUN_QA || env.isMainBranch
    ]
    // will merge Maps by overwriting left Map with values of the right Map
    def effectiveFlags = defaultFlags + flags

    def nodePoolName = "agents-n1-standard-32-physsd-${effectiveFlags.useStableNodePool ? 'stable' : 'preempt'}"

    String templateString = readTrusted(podspecPath)

    // Note: Templating is currently done via simple string substitution as this
    // is enough to solve all existing use cases. If need arises the templating
    // can be transparently changed to a more sophisticated YAML-merge based
    // approach as it is an implementation detail that the caller does not know.
    templateString = templateString.replaceAll('PODSPEC_TEMPLATE_NODE_POOL', nodePoolName)

    templateString
}

def combineFlakeResults(flakeFiles = []) {
    def flakes = []

    for (flakeFile in flakeFiles) {
        if (fileExists(flakeFile)) {
            flakes += readFile(flakeFile).split('\n')
        }
    }

    return flakes
}

def checkCodeCoverage() {
    jacoco(
        execPattern: '**/*.exec',
        classPattern: '**/target/classes',
        sourcePattern: '**/src/main/java,**/generated-sources/protobuf/java,**/generated-sources/assertj-assertions,**/generated-sources/sbe',
        exclusionPattern: '**/io/camunda/zeebe/gateway/protocol/**,'
            + '**/*Encoder.class,**/*Decoder.class,**/MetaAttribute.class,'
            + '**/io/camunda/zeebe/protocol/record/**/*Assert.class,**/io/camunda/zeebe/protocol/record/Assertions.class,', // classes from generated resources
        runAlways: true
    )
    zip zipFile: "test-coverage-reports.zip", archive: true, glob: '**/target/site/jacoco/**'
}
