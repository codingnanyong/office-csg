pipeline {
    agent any

    environment {
        dotnet = 'C:\\Program Files\\dotnet\\dotnet.exe'
    }

    stages {
        stage('Checkout') {
            steps {
                git '${GIT_REPO_URL}'
            }
        }
        stage('Build') {
            steps {
                bat 'dotnet build DataWarehouse\\DataWarehouse.sln --configuration Release'
            }
        }
        stage('Unit Test') {
            steps {
                bat 'dotnet test DataWarehouse\\DataWarehouse.Test\\DataWarehouse.Test.csproj --configuration Release'
            }
        }
        stage('Release') {
            steps {
                bat 'dotnet publish DataWarehouse\\DataWarehouse.OpenApi\\DataWarehouse.OpenApi.csproj --output DataWarehouse\\DataWarehouse.OpenApi\\bin\\publish'
            }
        }
        stage('Deploy') {
            steps {
                bat 'schtasks /end /tn "FdwWebApi"'
                bat 'copy /y "DataWarehouse\\DataWarehouse.OpenApi\\bin\\publish\\*.*" "C:\\apps\\MI\\FdwWebApi\\publish"'
                bat 'schtasks /run /tn "FdwWebApi"'
            }
        }
    }
}
