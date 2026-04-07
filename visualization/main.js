/**
 * Mvolo Master Dashboard Controller
 * Handles platform switching and state animations
 */

const DASHBOARDS = {
    bol: 'bol-dashboard/dashboard-proper/bol.html',
    shopify: 'shopify-dashboard/dashboard-proper/shopify.html'
};

document.addEventListener('DOMContentLoaded', () => {
    const toggle = document.getElementById('platformToggle');
    const frame = document.getElementById('dashboardFrame');
    const options = document.querySelectorAll('.toggle-option');
    const bgText = document.getElementById('bgText');
    const body = document.body;

    // Default Initialization
    body.className = 'is-bol';

    // Click handler for the Toggle Container
    toggle.addEventListener('click', () => {
        const currentActive = document.querySelector('.toggle-option.active');
        const nextPlatform = currentActive.dataset.platform === 'bol' ? 'shopify' : 'bol';
        
        switchPlatform(nextPlatform);
    });

    /**
     * Switch between platform contexts
     * @param {string} platform - 'bol' or 'shopify' 
     */
    function switchPlatform(platform) {
        // 1. Update UI Active State
        options.forEach(opt => {
            opt.classList.toggle('active', opt.dataset.platform === platform);
        });

        // 2. Update Body Class for BG Animation Class Switch
        body.className = `is-${platform}`;
        bgText.textContent = platform === 'bol' ? 'BOL.COM' : 'SHOPIFY';

        // 3. Smooth iFrame Swap Animation
        frame.classList.add('fade-out');
        
        setTimeout(() => {
            // Hot swap the iframe source
            frame.src = DASHBOARDS[platform];
            
            // Fade-in only after content starts loading 
            frame.onload = () => {
                frame.classList.remove('fade-out');
            };
        }, 300);
    }
});
