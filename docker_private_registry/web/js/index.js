/**
 * Index Page JavaScript
 * Handles footer insertion and Registry URL copy functionality
 */

/**
 * Copy Registry URL to clipboard
 */
function copyRegistryUrl(event) {
    if (event) {
        event.stopPropagation();
        event.preventDefault();
    }
    
    // 배포 시 index.html 또는 서버에서 window.REGISTRY_HOST 설정. 미설정 시 플레이스홀더 사용
const url = (typeof window !== 'undefined' && window.REGISTRY_HOST) ? ('https://' + window.REGISTRY_HOST + ':5000') : 'https://REGISTRY_HOST:5000';
    const copyButton = document.getElementById('copyButton');
    if (!copyButton) {
        console.error('Copy button not found');
        return;
    }
    
    const originalText = copyButton.textContent;
    
    // Try modern clipboard API first
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(url).then(function() {
            // Show success feedback
            copyButton.textContent = 'Copied!';
            copyButton.style.background = 'rgba(74, 222, 128, 0.3)';
            
            // Reset after 2 seconds
            setTimeout(function() {
                copyButton.textContent = originalText;
                copyButton.style.background = 'rgba(255, 255, 255, 0.2)';
            }, 2000);
        }).catch(function(err) {
            console.error('Clipboard API failed:', err);
            // Fallback to execCommand
            fallbackCopy(url, copyButton, originalText);
        });
    } else {
        // Fallback for browsers without clipboard API
        fallbackCopy(url, copyButton, originalText);
    }
}

/**
 * Fallback copy function using execCommand
 */
function fallbackCopy(url, copyButton, originalText) {
    const textArea = document.createElement('textarea');
    textArea.value = url;
    textArea.style.position = 'fixed';
    textArea.style.top = '0';
    textArea.style.left = '0';
    textArea.style.width = '2em';
    textArea.style.height = '2em';
    textArea.style.padding = '0';
    textArea.style.border = 'none';
    textArea.style.outline = 'none';
    textArea.style.boxShadow = 'none';
    textArea.style.background = 'transparent';
    textArea.style.opacity = '0';
    textArea.style.zIndex = '-1';
    
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();
    
    try {
        const successful = document.execCommand('copy');
        if (successful) {
            copyButton.textContent = 'Copied!';
            copyButton.style.background = 'rgba(74, 222, 128, 0.3)';
            setTimeout(function() {
                copyButton.textContent = originalText;
                copyButton.style.background = 'rgba(255, 255, 255, 0.2)';
            }, 2000);
        } else {
            throw new Error('execCommand failed');
        }
    } catch (err) {
        console.error('Fallback copy failed:', err);
        copyButton.textContent = 'Failed';
        copyButton.style.background = 'rgba(239, 68, 68, 0.3)';
        setTimeout(function() {
            copyButton.textContent = originalText;
            copyButton.style.background = 'rgba(255, 255, 255, 0.2)';
        }, 2000);
    }
    
    document.body.removeChild(textArea);
}

/**
 * Load registry statistics
 */
async function loadRegistryStatistics() {
    const statsElement = document.getElementById('registryStats');
    const loadingElement = document.getElementById('statsLoading');
    
    try {
        // Show stats section
        statsElement.style.display = 'block';
        
        // Fetch repository list
        const response = await fetch('/api/v2/_catalog');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        const repositories = data.repositories || [];
        
        // Update total images
        document.getElementById('totalImages').textContent = repositories.length;
        
        // Estimate size (rough calculation: avg 150MB per image)
        const estimatedSizeMB = repositories.length * 150;
        const estimatedSizeGB = (estimatedSizeMB / 1024).toFixed(1);
        document.getElementById('estimatedSize').textContent = `~${estimatedSizeGB} GB`;
        
        // Calculate total tags across all repositories
        let totalTags = 0;
        const tagPromises = repositories.map(async repo => {
            try {
                const tagResponse = await fetch(`/api/v2/${repo}/tags/list`);
                if (tagResponse.ok) {
                    const tagData = await tagResponse.json();
                    return tagData.tags ? tagData.tags.length : 0;
                }
                return 0;
            } catch (error) {
                console.warn(`Failed to fetch tags for ${repo}:`, error);
                return 0;
            }
        });
        
        const tagCounts = await Promise.all(tagPromises);
        totalTags = tagCounts.reduce((sum, count) => sum + count, 0);
        document.getElementById('totalTags').textContent = totalTags;
        
        // Get largest repository name (by character length as rough estimate)
        const largestRepo = repositories.reduce((a, b) => a.length > b.length ? a : b, '');
        document.getElementById('largestImage').textContent = largestRepo || 'N/A';
        
        // Hide loading
        loadingElement.style.display = 'none';
        
    } catch (error) {
        console.error('Failed to load statistics:', error);
        loadingElement.innerHTML = '<p style="color: #dc3545;">Failed to load statistics</p>';
    }
}

/**
 * Initialize page when DOM is ready
 */
document.addEventListener('DOMContentLoaded', function() {
    // Insert footer with custom title
    insertFooter('Docker Private Registry Resources Server');
    
    // Load registry statistics
    loadRegistryStatistics();
});
