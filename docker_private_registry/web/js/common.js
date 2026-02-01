/**
 * Common JavaScript for Docker Registry Web Interface
 */

// Footer configuration
const FOOTER_CONFIG = {
    author: 'codingnanyong',
    year: '2026'
};

/**
 * Insert common footer into the page
 * @param {string} title - Footer title text
 */
function insertFooter(title = 'Docker Private Registry Resources Server') {
    const footer = document.createElement('div');
    footer.className = 'footer';
    footer.innerHTML = `
        <p>${title}</p>
        <p style="margin-top: 10px; font-size: 0.9em; opacity: 0.8;">
            Created by <strong>${FOOTER_CONFIG.author}</strong> | ${FOOTER_CONFIG.year}
        </p>
    `;
    
    // Find container and append footer
    const container = document.querySelector('.container');
    if (container) {
        container.appendChild(footer);
    }
}

/**
 * Initialize common components when DOM is ready
 */
document.addEventListener('DOMContentLoaded', function() {
    // Footer will be inserted by each page if needed
    // This allows pages to customize the footer title
});
