pipeline{
    agent any
    stages{
        stage('Intro'){
            steps{
                sh 'echo "Hello, Jenkins and Github"'
                sh 'echo "Github hook test"'
            }
        }
        stage('Build'){
            steps{
                sh 'echo "Running maven clean install"'
                sh 'mvn clean install'
            }
        }
    }
}