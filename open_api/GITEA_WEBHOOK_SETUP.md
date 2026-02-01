# Gitea Webhook Setup Guide

## Overview

This document explains how to connect Gitea repositories with Jenkins through webhooks. It configures automatic Jenkins build triggers when code changes occur.

## Prerequisites

- Access to Gitea server
- Access to Jenkins server
- Gitea plugin installed in Jenkins
- Jenkins URL that can receive webhooks

## 1. Jenkins Configuration

### 1.1 Install Gitea Plugin

1. Jenkins Management → Plugin Management
2. Search and install "Gitea Integration" plugin
3. Restart Jenkins

### 1.2 Create Jenkins Job

1. Access Jenkins dashboard
2. Click "New Item"
3. Select "Pipeline"
4. Job name: Enter `openapi-blue-green-deploy`
5. Click "OK"

### 1.3 Pipeline Configuration

**Pipeline Script Example:**

```groovy
pipeline {
    agent any

    environment {
        REPO_URL = '{GITEA_REPO_URL}'
        BRANCH = 'main'
    }

    triggers {
        // Gitea webhook trigger
        GenericTrigger(
            genericVariables: [
                [key: 'ref', value: '$.ref'],
                [key: 'repository', value: '$.repository.name'],
                [key: 'pusher', value: '$.pusher.name']
            ],
            causeString: 'Triggered by Gitea webhook',
            token: 'your-webhook-token-here',
            regexpFilterText: '$ref',
            regexpFilterExpression: 'refs/heads/main'
        )
    }

    stages {
        stage('Checkout') {
            steps {
                echo "Triggered by: ${pusher}"
                echo "Repository: ${repository}"
                echo "Branch: ${ref}"

                checkout([
                    $class: 'GitSCM',
                    branches: [[name: '*/main']],
                    userRemoteConfigs: [[
                        url: REPO_URL,
                        credentialsId: 'gitea-credentials'
                    ]]
                ])
            }
        }

        stage('Build') {
            steps {
                echo 'Building OpenAPI application...'
                sh 'docker-compose -f docker/docker-compose.ops.yml build'
            }
        }

        stage('Test') {
            steps {
                echo 'Running tests...'
                sh 'python -m pytest app/tests/'
            }
        }

        stage('Deploy') {
            steps {
                echo 'Deploying with Blue-Green strategy...'
                sh 'bash scripts/deploy.sh'
            }
        }
    }

    post {
        always {
            echo 'Build completed'
        }
        success {
            echo 'Deployment successful!'
        }
        failure {
            echo 'Deployment failed!'
        }
    }
}
```

### 1.4 Configure Credentials

1. Jenkins Management → Credentials → System → Global credentials
2. Click "Add Credentials"
3. Kind: Select "Username with password"
4. ID: `gitea-credentials`
5. Username: Gitea username
6. Password: Gitea password or token
7. Click "OK"

## 2. Gitea Webhook Configuration

### 2.1 Access Repository Settings

1. Navigate to the repository in Gitea
2. Repository Settings → Webhooks
3. "Add Webhook" → Select "Gitea"

### 2.2 Webhook Configuration

**Basic Settings:**

- **Target URL**: `http://your-jenkins-url/generic-webhook-trigger/invoke`
- **HTTP Method**: POST
- **Content Type**: application/json
- **Secret**: Set same as token configured in Jenkins

**Trigger Settings:**

- ✅ **Push Events**: Checked
- ✅ **Create Events**: Checked (Optional)
- ✅ **Delete Events**: Checked (Optional)
- ✅ **Fork Events**: Unchecked
- ✅ **Issues Events**: Unchecked
- ✅ **Issue Assign Events**: Unchecked
- ✅ **Issue Label Events**: Unchecked
- ✅ **Issue Milestone Events**: Unchecked
- ✅ **Pull Request Events**: Checked (Optional)
- ✅ **Pull Request Assign Events**: Unchecked
- ✅ **Pull Request Label Events**: Unchecked
- ✅ **Pull Request Milestone Events**: Unchecked
- ✅ **Pull Request Comment Events**: Unchecked
- ✅ **Pull Request Review Events**: Unchecked
- ✅ **Repository Events**: Unchecked
- ✅ **Release Events**: Checked (If you want builds on releases)

**Branch Filter:**

- **Branch Filter**: `main` (Only trigger on main branch)

### 2.3 Test Webhook

1. Click "Test Delivery" button
2. Select "Push" event
3. Execute "Test Delivery"
4. Verify that build is triggered in Jenkins

## 3. Security Configuration

### 3.1 Jenkins Security

```groovy
// Add in Jenkins system configuration
// Jenkins Management → Configure System → Global properties
// Add Environment variables:
// GITEA_WEBHOOK_TOKEN = your-secure-token
```

### 3.2 Generate Gitea Token

1. Gitea User Settings → Applications
2. Click "Generate New Token"
3. Token Name: `jenkins-webhook`
4. Permissions: Check `repo`
5. Generate token and store securely

## 4. Troubleshooting

### 4.1 Webhook Not Triggering

1. **Verify Jenkins URL**

   - Check if Jenkins is accessible from external sources
   - Verify firewall settings

2. **Verify Token**

   - Ensure tokens match between Gitea and Jenkins
   - Verify token has correct permissions

3. **Check Logs**
   - Check Jenkins build logs
   - Check Gitea webhook delivery logs

### 4.2 Authentication Errors

1. **Verify Credentials**

   - Ensure Jenkins credentials are properly configured
   - Verify Gitea username/password

2. **Verify Permissions**
   - Ensure Gitea token has repository access permissions

## 5. Advanced Configuration

### 5.1 Conditional Builds

```groovy
// Build only when specific files are changed
triggers {
    GenericTrigger(
        genericVariables: [
            [key: 'ref', value: '$.ref'],
            [key: 'files', value: '$.commits[*].modified[*]']
        ],
        causeString: 'Triggered by Gitea webhook',
        token: 'your-webhook-token-here',
        regexpFilterText: '$files',
        regexpFilterExpression: '.*\\.(py|js|md)$'  // Only Python, JS, Markdown files
    )
}
```

### 5.2 Multi-Branch Support

```groovy
// Support multiple branches
triggers {
    GenericTrigger(
        genericVariables: [
            [key: 'ref', value: '$.ref'],
            [key: 'branch', value: '$.ref?replace("refs/heads/", "")']
        ],
        causeString: 'Triggered by Gitea webhook',
        token: 'your-webhook-token-here',
        regexpFilterText: '$ref',
        regexpFilterExpression: 'refs/heads/(main|develop|feature/.*)'
    )
}
```

## 6. Monitoring

### 6.1 Webhook Status Monitoring

- Check delivery status in Gitea repository settings → Webhooks
- Failed webhooks can be resent

### 6.2 Jenkins Build Monitoring

- Check build status in Jenkins dashboard
- Review build history and logs

---

**After Setup**: Jenkins builds will start automatically when code is pushed!

**Important Notes**:

- Store tokens securely
- Ensure Jenkins URL is accessible from external sources
- Verify firewall settings
