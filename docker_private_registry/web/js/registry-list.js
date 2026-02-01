/**
 * Registry List Page JavaScript
 * Handles repository listing, Dockerfile download, and auto-refresh functionality
 */

// Docker Registry host (API/이미지 주소용 - 5000 포트)
const REGISTRY_HOST = window.location.hostname + ':5000';

// Auto-refresh configuration
let autoRefreshInterval = null;
let countdownInterval = null;
let isAutoRefreshEnabled = true;
const AUTO_REFRESH_INTERVAL = 180000; // 3 minutes (180 seconds)
let refreshCountdown = AUTO_REFRESH_INTERVAL / 1000; // seconds

/**
 * Load repositories from Docker Registry API
 */
async function loadRepositories() {
    const loading = document.getElementById('loading');
    const error = document.getElementById('error');
    const stats = document.getElementById('stats');
    const repoList = document.getElementById('repoList');
    const topControls = document.querySelector('.top-controls');
    const searchControls = document.querySelector('.search-controls');
    
    loading.style.display = 'block';
    error.style.display = 'none';
    stats.style.display = 'none';
    repoList.style.display = 'none';
    if (topControls) topControls.style.display = 'none';
    if (searchControls) searchControls.style.display = 'none';
    
    try {
        const response = await fetch('/api/v2/_catalog');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        
        loading.style.display = 'none';
        
        if (data.repositories && data.repositories.length > 0) {
            // Filter out repositories with no tags (empty repositories)
            const reposWithTags = await Promise.all(
                data.repositories.map(async (repo) => {
                    try {
                        const tagsResponse = await fetch(`/api/v2/${repo}/tags/list`);
                        if (tagsResponse.ok) {
                            const tagsData = await tagsResponse.json();
                            if (tagsData.tags && tagsData.tags.length > 0) {
                                return { name: repo, tags: tagsData.tags };
                            }
                        }
                        return null; // No tags or error
                    } catch (err) {
                        console.warn(`Failed to check tags for ${repo}:`, err);
                        return null;
                    }
                })
            );
            
            const validRepos = reposWithTags.filter(repo => repo !== null);
            
            if (validRepos.length > 0) {
                repoList.innerHTML = validRepos.map(repo => {
                    // Tag 셀렉트 박스 HTML (태그별 다운로드를 위해)
                    const defaultTag = repo.tags.includes('latest') ? 'latest' : repo.tags[0];
                    // latest 태그를 항상 맨 위에 오도록 정렬
                    const sortedTags = [...repo.tags].sort((a, b) => {
                        if (a === 'latest' && b !== 'latest') return -1;
                        if (a !== 'latest' && b === 'latest') return 1;
                        return a.localeCompare(b);
                    });
                    const tagOptions = sortedTags.map(tag => `
                        <div class="tag-option ${tag === defaultTag ? 'selected' : ''}" data-tag="${tag}">
                            ${tag}
                        </div>
                    `).join('');

                    return `
                    <div class="repo-card">
                        <div class="repo-card-header">
                            <div class="repo-card-content">
                                <div class="repo-name">${repo.name}</div>
                                <div class="repo-tags-row">
                                    <span class="tag-label">Tag</span>
                                    <div class="tag-select" data-repo="${repo.name}" data-selected-tag="${defaultTag}">
                                        <button type="button" class="tag-select-toggle">
                                            <span class="tag-select-value">${defaultTag}</span>
                                        </button>
                                        <div class="tag-select-options">
                                            ${tagOptions}
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div class="repo-card-actions">
                                <button class="delete-btn" onclick="event.stopPropagation(); deleteRepository('${repo.name}')" title="Delete Repository">
                                    🗑️ Delete
                                </button>
                            </div>
                        </div>
                        <div class="download-buttons">
                            <button class="download-btn dockerfile-btn" onclick="event.stopPropagation(); downloadDockerfile('${repo.name}', getSelectedTag('${repo.name}'))" title="Download Dockerfile">
                                <span class="btn-icon">🐳</span>
                                <span class="btn-text">Dockerfile</span>
                            </button>
                            <button class="command-btn" onclick="event.stopPropagation(); showDownloadCommands('${repo.name}', getSelectedTag('${repo.name}'))" title="Show download commands">
                                <span class="btn-icon">💻</span>
                                <span class="btn-text">Commands</span>
                            </button>
                        </div>
                    </div>
                    `;
                }).join('');

                // 커스텀 태그 드롭다운 초기화
                initTagDropdowns();

                repoList.style.display = 'grid';
            } else {
                repoList.innerHTML = '<div class="no-repos">No repositories with tags found</div>';
                repoList.style.display = 'block';
            }
        } else {
            repoList.innerHTML = '<div class="no-repos">No repositories found</div>';
            repoList.style.display = 'block';
        }
        
        if (topControls) topControls.style.display = 'flex';
        if (searchControls) searchControls.style.display = 'block';
        stats.style.display = 'none'; // stats는 이제 검색 통계로 대체되므로 숨김
        updateSearchStats();
    } catch (err) {
        loading.style.display = 'none';
        error.innerHTML = `<strong>Error:</strong> ${err.message}`;
        error.style.display = 'block';
        if (topControls) topControls.style.display = 'flex';
        if (searchControls) searchControls.style.display = 'block';
        stats.style.display = 'none'; // stats는 검색 통계로 대체
    }
}

/**
 * 커스텀 태그 드롭다운 초기화
 */
function initTagDropdowns() {
    const dropdowns = document.querySelectorAll('.tag-select');
    
    dropdowns.forEach(dropdown => {
        const toggle = dropdown.querySelector('.tag-select-toggle');
        const valueSpan = dropdown.querySelector('.tag-select-value');
        const options = dropdown.querySelectorAll('.tag-option');
        
        if (!toggle || !valueSpan || options.length === 0) {
            return;
        }
        
        // 열기/닫기 토글
        toggle.onclick = (event) => {
            event.stopPropagation();
            
            // 다른 드롭다운은 닫기
            document.querySelectorAll('.tag-select.open').forEach(other => {
                if (other !== dropdown) {
                    other.classList.remove('open');
                }
            });
            
            dropdown.classList.toggle('open');
        };
        
        // 옵션 선택
        options.forEach(option => {
            option.onclick = (event) => {
                event.stopPropagation();
                const tag = option.getAttribute('data-tag');
                if (!tag) return;
                
                dropdown.dataset.selectedTag = tag;
                valueSpan.textContent = tag;
                
                // 선택 상태 업데이트
                options.forEach(opt => opt.classList.toggle('selected', opt === option));
                
                dropdown.classList.remove('open');
            };
        });
    });
    
    // 바깥 클릭 시 모든 드롭다운 닫기 (한 번만 등록)
    if (!window._tagDropdownOutsideClickBound) {
        document.addEventListener('click', () => {
            document.querySelectorAll('.tag-select.open').forEach(d => d.classList.remove('open'));
        });
        window._tagDropdownOutsideClickBound = true;
    }
}

/**
 * 현재 레포지토리에서 선택된 태그 조회
 */
function getSelectedTag(repoName) {
    const dropdown = document.querySelector(`.tag-select[data-repo="${repoName}"]`);
    if (!dropdown) return null;
    
    const selectedTag = dropdown.dataset.selectedTag;
    if (selectedTag) return selectedTag;
    
    const valueSpan = dropdown.querySelector('.tag-select-value');
    return valueSpan ? valueSpan.textContent : null;
}

/**
 * Generate and download docker-compose.yml template for a repository
 * Creates a basic template based on the selected image and tag
 */
async function downloadDockerCompose(repoName, tag) {
    try {
        const selectedTag = tag || getSelectedTag(repoName) || 'latest';
        const imageName = `${REGISTRY_HOST}/${repoName}:${selectedTag}`;
        
        // Generate service name from repo name (last part after /)
        const serviceName = repoName.split('/').pop().replace(/[^a-z0-9]/gi, '-').toLowerCase();
        
        // Generate docker-compose.yml template
        const dockerComposeContent = `version: '3.8'

services:
  ${serviceName}:
    image: ${imageName}
    container_name: ${serviceName}
    restart: unless-stopped
    ports:
      - "8080:8080"  # Adjust ports based on your application
    environment:
      # Add your environment variables here
      # Example:
      # - ENV_VAR=value
    volumes:
      # Add volume mounts if needed
      # Example:
      # - ./data:/app/data
    networks:
      - app_network
    healthcheck:
      # Add healthcheck based on your application
      # Example for HTTP service:
      # test: ["CMD-SHELL", "curl -f http://localhost:8080/health || exit 1"]
      # interval: 30s
      # timeout: 10s
      # retries: 3
      # start_period: 30s

networks:
  app_network:
    driver: bridge
    # external: true  # Uncomment if using external network

# volumes:
#   app_data:  # Uncomment and add volumes if needed
`;

        downloadFile(dockerComposeContent, 'docker-compose.yml');
        
    } catch (error) {
        alert(`Error generating docker-compose.yml for ${repoName}: ${error.message}`);
    }
}

/**
 * Show download commands for a repository
 * Displays curl/wget commands to download files from Gitea or web interface
 */
function showDownloadCommands(repoName, tag) {
    const selectedTag = tag || getSelectedTag(repoName) || 'latest';
    const imageName = `${REGISTRY_HOST}/${repoName}:${selectedTag}`;
    
    const commands = `📥 Download Commands for ${repoName}:${selectedTag}

🐳 Pull Docker Image:
docker pull ${imageName}

📝 Generate Dockerfile:
# Dockerfile will be reconstructed from image manifest
# Use the "Dockerfile" button in the UI to download

💡 Quick Start:
# 1. Pull the image
docker pull ${imageName}

# 2. Run the container
docker run -d --name ${repoName.split('/').pop()} \\
  -p 8080:8080 \\
  ${imageName}

# Or use docker-compose (after generating template)
# docker-compose up -d`;
    
    // Escape HTML special characters in commands
    const escapedCommands = commands
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
    
    // Create modal
    const modal = document.createElement('div');
    modal.className = 'command-modal';
    modal.innerHTML = `
        <div class="command-modal-content">
            <div class="command-modal-header">
                <h3>💻 Download Commands</h3>
                <button class="command-modal-close" onclick="this.closest('.command-modal').remove()">×</button>
            </div>
            <div class="command-modal-body">
                <pre class="command-code">${escapedCommands}</pre>
                <div class="command-actions">
                    <button class="copy-btn" onclick="copyCommands('${repoName.replace(/'/g, "\\'")}')">📋 Copy Commands</button>
                    <button class="copy-btn" onclick="copyAllCommands('${repoName.replace(/'/g, "\\'")}')">📋 Copy All</button>
                </div>
            </div>
        </div>
    `;
    
    document.body.appendChild(modal);
    modal.style.display = 'flex';
    
    // Close on outside click
    modal.addEventListener('click', (e) => {
        if (e.target === modal) {
            modal.remove();
        }
    });
    
    // Close on Escape key
    const closeOnEscape = (e) => {
        if (e.key === 'Escape') {
            modal.remove();
            document.removeEventListener('keydown', closeOnEscape);
        }
    };
    document.addEventListener('keydown', closeOnEscape);
}

/**
 * Copy commands to clipboard (one-liner)
 */
function copyCommands(repoName) {
    const selectedTag = getSelectedTag(repoName) || 'latest';
    const imageName = `${REGISTRY_HOST}/${repoName}:${selectedTag}`;
    
    const commands = `docker pull ${imageName}`;
    
    copyToClipboard(commands);
}

/**
 * Copy all commands to clipboard
 */
function copyAllCommands(repoName) {
    const selectedTag = getSelectedTag(repoName) || 'latest';
    const imageName = `${REGISTRY_HOST}/${repoName}:${selectedTag}`;
    
    const allCommands = `📥 Download Commands for ${repoName}:${selectedTag}

🐳 Pull Docker Image:
docker pull ${imageName}

📝 Generate Dockerfile:
# Dockerfile will be reconstructed from image manifest
# Use the "Dockerfile" button in the UI to download

📋 Generate docker-compose.yml:
# docker-compose.yml template will be generated
# Use the "docker-compose.yml" button in the UI to download

💡 Quick Start:
# 1. Pull the image
docker pull ${imageName}

# 2. Run the container
docker run -d --name ${repoName.split('/').pop()} \\
  -p 8080:8080 \\
  ${imageName}

# Or use docker-compose (after generating template)
# docker-compose up -d`;
    
    copyToClipboard(allCommands);
}

/**
 * Copy text to clipboard
 */
function copyToClipboard(text) {
    if (navigator.clipboard) {
        navigator.clipboard.writeText(text).then(() => {
            alert('✅ Commands copied to clipboard!');
        }).catch(err => {
            console.error('Failed to copy:', err);
            // Fallback: select text
            const textarea = document.createElement('textarea');
            textarea.value = text;
            document.body.appendChild(textarea);
            textarea.select();
            document.execCommand('copy');
            document.body.removeChild(textarea);
            alert('✅ Commands copied to clipboard!');
        });
    } else {
        // Fallback for older browsers
        const textarea = document.createElement('textarea');
        textarea.value = text;
        document.body.appendChild(textarea);
        textarea.select();
        document.execCommand('copy');
        document.body.removeChild(textarea);
        alert('✅ Commands copied to clipboard!');
    }
}

/**
 * Download Dockerfile for a repository
 * Reconstructs Dockerfile from image manifest and config history
 */
async function downloadDockerfile(repoName, tag) {
    try {
        const selectedTag = tag || getSelectedTag(repoName);
        console.log(`Reconstructing Dockerfile from manifest for ${repoName} (tag=${selectedTag || 'auto'})...`);
        
        // Get tags list (사용자가 선택한 태그가 없으면 목록에서 결정)
        let effectiveTag = selectedTag;
        if (!effectiveTag) {
            const tagsResponse = await fetch(`/api/v2/${repoName}/tags/list`);
            if (!tagsResponse.ok) {
                throw new Error(`Failed to get tags: ${tagsResponse.status}`);
            }
            const tagsData = await tagsResponse.json();
            
            if (!tagsData.tags || tagsData.tags.length === 0) {
                alert(`No tags found for ${repoName}`);
                return;
            }
            
            // Use 'latest' tag if available, otherwise use first tag
            effectiveTag = tagsData.tags.includes('latest') ? 'latest' : tagsData.tags[0];
        }
        
        // Get manifest
        const manifestResponse = await fetch(`/api/v2/${repoName}/manifests/${effectiveTag}`, {
            headers: {
                'Accept': 'application/vnd.docker.distribution.manifest.v2+json'
            }
        });
        
        if (!manifestResponse.ok) {
            throw new Error(`Failed to get manifest: ${manifestResponse.status}`);
        }
        
        const manifest = await manifestResponse.json();
        const configDigest = manifest.config?.digest;
        
        if (!configDigest) {
            throw new Error('Config digest not found in manifest');
        }
        
        // Get config blob (contains history information)
        const configResponse = await fetch(`/api/v2/${repoName}/blobs/${configDigest}`);
        if (!configResponse.ok) {
            throw new Error(`Failed to get config: ${configResponse.status}`);
        }
        
        const config = await configResponse.json();
        
        // Reconstruct Dockerfile
        let dockerfileContent = reconstructDockerfile(config, repoName, effectiveTag);
        
        // Download file - ensure no extension is added
        downloadFile(dockerfileContent, 'Dockerfile');
        
    } catch (err) {
        alert(`Error downloading Dockerfile for ${repoName}: ${err.message}`);
    }
}

/**
 * Reconstruct Dockerfile from config history and config data
 */
function reconstructDockerfile(config, repoName, tag) {
    let dockerfileContent = '';
    
    // Extract from history
    if (config.history && config.history.length > 0) {
        const history = config.history;
        
        // Process history in reverse order (newest first)
        for (let i = history.length - 1; i >= 0; i--) {
            const entry = history[i];
            if (entry.created_by) {
                const createdBy = entry.created_by;
                dockerfileContent += extractDockerfileCommands(createdBy, dockerfileContent);
            }
        }
    }
    
    // Extract from config directly
    if (config.config) {
        dockerfileContent += extractConfigData(config.config);
    }
    
    // Add FROM if missing
    if (!dockerfileContent.includes('FROM')) {
        const baseImage = config.config?.Image || repoName;
        dockerfileContent = `FROM ${baseImage}\n\n${dockerfileContent}`;
    }
    
    return dockerfileContent;
}

/**
 * Extract Dockerfile commands from created_by string
 */
function extractDockerfileCommands(createdBy, existingContent) {
    let content = '';
    
    // FROM command
    if ((createdBy.includes('FROM') || createdBy.includes('from')) && !existingContent.includes('FROM')) {
        const fromMatch = createdBy.match(/FROM\s+([^\s]+)/i);
        if (fromMatch) {
            content += `FROM ${fromMatch[1]}\n\n`;
        }
    }
    
    // ENV command
    if (createdBy.includes('ENV') || createdBy.includes('env')) {
        const envMatch = createdBy.match(/ENV\s+([^\n]+)/i);
        if (envMatch) {
            content += `ENV ${envMatch[1]}\n`;
        }
    }
    
    // COPY command
    if (createdBy.includes('COPY') || createdBy.includes('copy')) {
        const copyMatch = createdBy.match(/COPY\s+([^\n]+)/i);
        if (copyMatch) {
            content += `COPY ${copyMatch[1]}\n`;
        }
    }
    
    // ADD command
    if (createdBy.includes('ADD') || createdBy.includes('add')) {
        const addMatch = createdBy.match(/ADD\s+([^\n]+)/i);
        if (addMatch) {
            content += `ADD ${addMatch[1]}\n`;
        }
    }
    
    // RUN command (commented out as it's already executed)
    if (createdBy.includes('RUN') || createdBy.includes('run')) {
        const runMatch = createdBy.match(/RUN\s+([^\n]+)/i);
        if (runMatch) {
            content += `# RUN ${runMatch[1]}\n`;
        }
    }
    
    // EXPOSE command
    if (createdBy.includes('EXPOSE') || createdBy.includes('expose')) {
        const exposeMatch = createdBy.match(/EXPOSE\s+([^\n]+)/i);
        if (exposeMatch) {
            content += `EXPOSE ${exposeMatch[1]}\n`;
        }
    }
    
    // CMD command
    if (createdBy.includes('CMD') || createdBy.includes('cmd')) {
        const cmdMatch = createdBy.match(/CMD\s+\[([^\]]+)\]/i);
        if (cmdMatch) {
            content += `CMD [${cmdMatch[1]}]\n`;
        } else {
            const cmdMatch2 = createdBy.match(/CMD\s+([^\n]+)/i);
            if (cmdMatch2) {
                content += `CMD ${cmdMatch2[1]}\n`;
            }
        }
    }
    
    // WORKDIR command
    if (createdBy.includes('WORKDIR') || createdBy.includes('workdir')) {
        const workdirMatch = createdBy.match(/WORKDIR\s+([^\n]+)/i);
        if (workdirMatch) {
            content += `WORKDIR ${workdirMatch[1]}\n`;
        }
    }
    
    return content;
}

/**
 * Extract data from config.config object
 */
function extractConfigData(configData) {
    let content = '';
    
    // ENV variables
    if (configData.Env && configData.Env.length > 0) {
        content += '\n# Environment variables\n';
        configData.Env.forEach(env => {
            const [key, value] = env.split('=');
            content += `ENV ${key}=${value}\n`;
        });
    }
    
    // Exposed ports
    if (configData.ExposedPorts) {
        content += '\n# Exposed ports\n';
        Object.keys(configData.ExposedPorts).forEach(port => {
            content += `EXPOSE ${port.replace('/tcp', '').replace('/udp', '')}\n`;
        });
    }
    
    // Cmd
    if (configData.Cmd && configData.Cmd.length > 0) {
        content += '\n# Command\n';
        content += `CMD ${JSON.stringify(configData.Cmd)}\n`;
    }
    
    return content;
}

/**
 * Download file as blob
 */
function downloadFile(content, filename) {
    try {
        // Method 1: Using application/octet-stream
        const blob = new Blob([content], { 
            type: 'application/octet-stream'
        });
        
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        
        // Set all possible attributes to ensure correct filename
        a.href = url;
        a.download = filename;
        a.setAttribute('download', filename);
        a.style.display = 'none';
        
        document.body.appendChild(a);
        a.click();
        
        // Cleanup
        setTimeout(() => {
            document.body.removeChild(a);
            window.URL.revokeObjectURL(url);
        }, 100);
        
    } catch (error) {
        // Fallback method: Create text file with specific headers
        console.warn('Primary download method failed, using fallback:', error);
        
        const blob = new Blob([content], { 
            type: 'text/plain;charset=utf-8' 
        });
        
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        a.click();
        
        setTimeout(() => {
            window.URL.revokeObjectURL(url);
        }, 100);
    }
}

/**
 * Start auto-refresh functionality
 */
function startAutoRefresh() {
    if (autoRefreshInterval) {
        clearInterval(autoRefreshInterval);
    }
    if (countdownInterval) {
        clearInterval(countdownInterval);
    }
    
    isAutoRefreshEnabled = true;
    refreshCountdown = AUTO_REFRESH_INTERVAL / 1000;
    updateAutoRefreshStatus();
    
    // Update countdown every second
    countdownInterval = setInterval(() => {
        if (!isAutoRefreshEnabled) {
            clearInterval(countdownInterval);
            return;
        }
        refreshCountdown--;
        if (refreshCountdown <= 0) {
            refreshCountdown = AUTO_REFRESH_INTERVAL / 1000;
        }
        updateAutoRefreshStatus();
    }, 1000);
    
    // Actual refresh every 3 minutes
    autoRefreshInterval = setInterval(() => {
        if (isAutoRefreshEnabled) {
            loadRepositories();
            refreshCountdown = AUTO_REFRESH_INTERVAL / 1000;
        }
    }, AUTO_REFRESH_INTERVAL);
}

/**
 * Stop auto-refresh functionality
 */
function stopAutoRefresh() {
    if (autoRefreshInterval) {
        clearInterval(autoRefreshInterval);
        autoRefreshInterval = null;
    }
    if (countdownInterval) {
        clearInterval(countdownInterval);
        countdownInterval = null;
    }
    isAutoRefreshEnabled = false;
    updateAutoRefreshStatus();
}

/**
 * Manual refresh - turns off auto-refresh and refreshes immediately
 */
function manualRefresh() {
    // Turn off auto-refresh
    const toggle = document.getElementById('autoRefreshToggle');
    if (toggle) {
        toggle.checked = false;
    }
    isAutoRefreshEnabled = false;
    stopAutoRefresh();
    
    // Refresh immediately
    loadRepositories();
}

/**
 * Toggle auto-refresh on/off
 */
function toggleAutoRefresh() {
    const toggle = document.getElementById('autoRefreshToggle');
    isAutoRefreshEnabled = toggle.checked;
    
    if (isAutoRefreshEnabled) {
        startAutoRefresh();
    } else {
        stopAutoRefresh();
    }
}

/**
 * Update auto-refresh status display
 */
function updateAutoRefreshStatus() {
    const toggle = document.getElementById('autoRefreshToggle');
    
    if (toggle) {
        toggle.checked = isAutoRefreshEnabled;
    }
}

/**
 * Delete repository from registry
 */
async function deleteRepository(repoName) {
    if (!confirm(`⚠️ ${repoName} 레포지토리를 완전히 삭제하시겠습니까?\n\n이 작업은 되돌릴 수 없습니다.`)) {
        return;
    }
    
    try {
        // Show loading state
        const deleteButtons = document.querySelectorAll(`button[onclick*="${repoName}"]`);
        deleteButtons.forEach(btn => {
            btn.disabled = true;
            btn.textContent = '삭제 중...';
        });
        
        // Get tags list
        const tagsResponse = await fetch(`/api/v2/${repoName}/tags/list`);
        if (!tagsResponse.ok) {
            const errorText = await tagsResponse.text().catch(() => '');
            throw new Error(`태그 목록 가져오기 실패: ${tagsResponse.status} ${tagsResponse.statusText}\n${errorText}`);
        }
        const tagsData = await tagsResponse.json();
        
        if (!tagsData.tags || tagsData.tags.length === 0) {
            // 태그가 없는 경우에도 레포지토리는 남아있을 수 있음
            // 이 경우 레포지토리 자체는 삭제할 수 없지만, 사용자에게 알림
            alert(`⚠️ ${repoName}에는 삭제할 태그가 없습니다.\n\n레포지토리 디렉토리는 Garbage Collection 후 제거됩니다.`);
            deleteButtons.forEach(btn => {
                btn.disabled = false;
                btn.textContent = '🗑️ Delete';
            });
            return;
        }
        
        // Delete each tag's manifest
        let deletedCount = 0;
        let failedTags = [];
        
        for (const tag of tagsData.tags) {
            try {
                // Get manifest with digest
                const manifestResponse = await fetch(`/api/v2/${repoName}/manifests/${tag}`, {
                    headers: {
                        'Accept': 'application/vnd.docker.distribution.manifest.v2+json'
                    }
                });
                
                if (!manifestResponse.ok) {
                    console.warn(`Failed to get manifest for ${repoName}:${tag}: ${manifestResponse.status}`);
                    failedTags.push(`${tag} (manifest 조회 실패: ${manifestResponse.status})`);
                    continue;
                }
                
                const digest = manifestResponse.headers.get('docker-content-digest');
                if (!digest) {
                    console.warn(`No digest found for ${repoName}:${tag}`);
                    failedTags.push(`${tag} (digest 없음)`);
                    continue;
                }
                
                // Delete manifest using digest
                const deleteResponse = await fetch(`/api/v2/${repoName}/manifests/${digest}`, {
                    method: 'DELETE',
                    headers: {
                        'Accept': 'application/vnd.docker.distribution.manifest.v2+json'
                    }
                });
                
                if (deleteResponse.ok || deleteResponse.status === 202) {
                    deletedCount++;
                    console.log(`Successfully deleted ${repoName}:${tag} (${digest})`);
                } else {
                    const errorText = await deleteResponse.text().catch(() => '');
                    console.error(`Failed to delete ${repoName}:${tag}: ${deleteResponse.status} ${errorText}`);
                    failedTags.push(`${tag} (삭제 실패: ${deleteResponse.status})`);
                }
            } catch (tagError) {
                console.error(`Error deleting tag ${tag}:`, tagError);
                failedTags.push(`${tag} (에러: ${tagError.message})`);
            }
        }
        
        if (deletedCount > 0) {
            let message = `✅ ${deletedCount}개의 태그가 삭제되었습니다.`;
            if (failedTags.length > 0) {
                message += `\n\n⚠️ 실패한 태그: ${failedTags.join(', ')}`;
            }
            
            // Show success message
            alert(message);
            
            // Show GC notification
            showGCNotification(repoName);
            
            // Refresh repository list after a delay (레포지토리가 사라지지 않을 수 있으므로 더 긴 대기)
            setTimeout(() => {
                loadRepositories();
            }, 2000);
            
        } else {
            throw new Error(`모든 태그 삭제에 실패했습니다.\n실패한 태그: ${failedTags.join(', ')}`);
        }
        
    } catch (error) {
        console.error(`Error deleting repository ${repoName}:`, error);
        alert(`❌ ${repoName} 삭제 실패:\n${error.message}`);
        
        // Reset button state
        const deleteButtons = document.querySelectorAll(`button[onclick*="${repoName}"]`);
        deleteButtons.forEach(btn => {
            btn.disabled = false;
            btn.textContent = '🗑️ Delete';
        });
    }
}

/**
 * Show garbage collection notification
 */
function showGCNotification(repoName) {
    // Remove existing notification
    const existingNotification = document.querySelector('.gc-notification');
    if (existingNotification) {
        existingNotification.remove();
    }
    
    // Create notification
    const notification = document.createElement('div');
    notification.className = 'gc-notification';
    notification.innerHTML = `
        <button class="close-btn" onclick="this.parentElement.remove()">×</button>
        <h4>🗑️ ${repoName} 삭제 완료</h4>
        <p><strong>Garbage Collection이 필요합니다:</strong></p>
        <code>docker exec registry registry garbage-collect /etc/docker/registry/config.yml</code>
        <br><br>
        <small>위 명령어를 실행하여 실제 디스크 공간을 정리하세요.</small>
    `;
    
    document.body.appendChild(notification);
    notification.style.display = 'block';
    
    // Auto hide after 10 seconds
    setTimeout(() => {
        if (notification.parentElement) {
            notification.remove();
        }
    }, 10000);
}

/**
 * Run garbage collection (show instructions)
 */
function runGarbageCollection() {
    const message = `🧹 Garbage Collection 실행\n\n` +
        `다음 명령어를 터미널에서 실행하세요:\n\n` +
        `docker exec registry registry garbage-collect /etc/docker/registry/config.yml\n\n` +
        `이 명령어는 삭제된 이미지들의 실제 데이터를 정리하여 디스크 공간을 확보합니다.`;
    
    alert(message);
    
    // Copy command to clipboard if possible
    if (navigator.clipboard) {
        navigator.clipboard.writeText('docker exec registry registry garbage-collect /etc/docker/registry/config.yml')
            .then(() => {
                console.log('GC command copied to clipboard');
            })
            .catch(err => {
                console.log('Could not copy command to clipboard');
            });
    }
}

/**
 * Filter repositories based on search term
 */
function filterRepositories(searchTerm) {
    const repoCards = document.querySelectorAll('.repo-card');
    let visibleCount = 0;
    
    repoCards.forEach(card => {
        const repoName = card.querySelector('.repo-name');
        if (repoName) {
            const name = repoName.textContent.toLowerCase();
            const matches = name.includes(searchTerm.toLowerCase());
            
            if (matches || searchTerm === '') {
                card.style.display = 'block';
                visibleCount++;
            } else {
                card.style.display = 'none';
            }
        }
    });
    
    // Update search statistics
    document.getElementById('searchCount').textContent = visibleCount;
}

/**
 * Update search statistics
 */
function updateSearchStats() {
    const repoCards = document.querySelectorAll('.repo-card');
    const totalCount = repoCards.length;
    const searchInput = document.getElementById('searchInput');
    
    document.getElementById('totalCount').textContent = totalCount;
    
    if (searchInput && searchInput.value) {
        filterRepositories(searchInput.value);
    } else {
        document.getElementById('searchCount').textContent = totalCount;
    }
}

/**
 * Setup search functionality
 */
function setupSearch() {
    const searchInput = document.getElementById('searchInput');
    if (searchInput) {
        // Real-time search as user types
        searchInput.addEventListener('input', function(e) {
            filterRepositories(e.target.value);
        });
        
        // Clear search on Escape key
        searchInput.addEventListener('keydown', function(e) {
            if (e.key === 'Escape') {
                e.target.value = '';
                filterRepositories('');
            }
        });
    }
}

/**
 * Initialize page when DOM is ready
 */
document.addEventListener('DOMContentLoaded', function() {
    // Load repositories on page load
    loadRepositories();
    
    // Setup auto-refresh toggle event listener
    const autoRefreshToggle = document.getElementById('autoRefreshToggle');
    if (autoRefreshToggle) {
        autoRefreshToggle.addEventListener('change', toggleAutoRefresh);
    }
    
    // Setup search functionality
    setupSearch();
    
    // Start auto-refresh
    startAutoRefresh();
    
    // Cleanup on page unload
    window.addEventListener('beforeunload', () => {
        stopAutoRefresh();
    });
    
    // Insert footer
    insertFooter('Docker Private Registry Web Interface');
});
