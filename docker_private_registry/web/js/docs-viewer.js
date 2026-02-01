/**
 * Documentation Viewer Page JavaScript
 * Handles document loading and Markdown rendering
 */

/**
 * Load and display selected document
 */
async function loadDocument() {
    const selector = document.getElementById('docSelector');
    const contentDiv = document.getElementById('docContent');
    const selectedDoc = selector.value;
    
    if (!selectedDoc) {
        contentDiv.innerHTML = '<p class="loading">Please select a document from the dropdown above.</p>';
        return;
    }
    
    contentDiv.innerHTML = '<p class="loading">Loading document...</p>';
    
    try {
        const response = await fetch(`/docs/${selectedDoc}`);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const markdown = await response.text();
        
        // Use Marked.js to convert markdown to HTML
        if (typeof marked !== 'undefined') {
            const html = marked.parse(markdown);
            contentDiv.className = 'markdown-body';
            contentDiv.innerHTML = html;
        } else {
            // Fallback if Marked.js is not loaded
            contentDiv.className = 'markdown-body';
            contentDiv.innerHTML = `<pre>${markdown}</pre>`;
        }
    } catch (error) {
        contentDiv.className = 'error';
        contentDiv.innerHTML = `<strong>Error loading document:</strong><br>${error.message}`;
    }
}

/**
 * Initialize page when DOM is ready
 */
document.addEventListener('DOMContentLoaded', function() {
    // Load document from URL parameter if present
    const urlParams = new URLSearchParams(window.location.search);
    const doc = urlParams.get('doc');
    if (doc) {
        document.getElementById('docSelector').value = doc;
        loadDocument();
    }
    
    // Insert footer
    insertFooter('Docker Private Registry Documentation');
});
