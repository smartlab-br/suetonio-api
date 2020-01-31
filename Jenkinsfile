#!groovy

pipeline {
    agent {
        label 'master'
    }

    stages {

        stage('Pull Docker Parent Image') {
            steps {
                img_parent_pull()
            }
        }

        stage('Execute unit tests') {
            agent {
                docker {
                    image 'smartlab/flask:latest'
                    args '-u root:root'
                    reuseNode true
                }
            }
            environment {
                PYTHONPATH = "${pwd()}/app:$PYTHONPATH"
                PYTHONDONTWRITEBYTECODE = 1
            }
            steps {
                executeUnitTests()
            }
            post {
               always {
                    junit 'app/test/report.xml'
                    step([$class: 'CoberturaPublisher', coberturaReportFile: 'app/test/coverage/coverage.xml'])
                }
            }
        }

        // stage('SonarQube analysis') {
        //     steps {
        //         sonarScanner()
        //     }
        // }

        stage('Build and Register Image') {
            steps {
                buildAndRegisterImage()
            }
        }
   }
}

def img_parent_pull() {
    //pull img docker
    def img_flask = docker.image('smartlab/flask:latest')
    img_flask.pull()
}

def executeUnitTests() {
    //dir ("app") {
    sh "pip3 install -Iv pandas==0.23.3 flask-restful==0.3.6 requests==2.21.0 PyYAML==5.1 Babel==2.6.0 impyla==0.15a1 kafka-python==1.4.7 redis==3.0.1"
    sh "pip3 install nose2"
    sh "nose2 --config app/test/nose2.cfg --with-cov --coverage-report xml --coverage-config app/test/coverage/.coveragerc"
    //}
}

def sonarScanner() {
    //dir ("app") {
     def scannerHome = tool 'sonar'
     withSonarQubeEnv('Sonar - MPT') {
        sh "${scannerHome}/bin/sonar-scanner -Dproject.settings=./sonar-project.properties"
     }
    //}
}

def buildAndRegisterImage() {
    docker.withRegistry("${DOCKER_REGISTRY}", 'docker-registry.mpt') {
        def img = docker.build("${NOME_IMAGEM_DOCKER}", "--no-cache --pull --rm .")
        img.push("${VERSAO}")
    }
}
