import streamlit as st
import zipfile
import io
from pathlib import Path

# Page configuration
st.set_page_config(
    page_title="Documentation - BigQuery Data Processor",
    page_icon="assets/web/icons8-hub-pulsar-gradient-32.png",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS - Same styling as main page
st.markdown("""
<style>
    /* Hide default Streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Hide image enlarge/fullscreen button - Only for logo images */
    /* Target logo specifically by checking if it's in the logo column */
    div[data-testid="column"]:nth-of-type(2) [data-testid="stImage"] button,
    div[data-testid="column"]:nth-of-type(2) [data-testid="stImage"] > div > button,
    div[data-testid="column"]:nth-of-type(2) [data-testid="stImage"] > div > div > button,
    div[data-testid="column"]:nth-of-type(2) [data-testid="stImage"] * button,
    div[data-testid="column"]:nth-of-type(2) button[title*="Fullscreen"],
    div[data-testid="column"]:nth-of-type(2) button[aria-label*="Fullscreen"],
    div[data-testid="column"]:nth-of-type(2) button[aria-label*="fullscreen"],
    div[data-testid="column"]:nth-of-type(2) button[aria-label*="Expand"],
    div[data-testid="column"]:nth-of-type(2) button[aria-label*="expand"],
    .logo-wrapper [data-testid="stImage"] button,
    .logo-wrapper [data-testid="stImage"] > div > button,
    .logo-wrapper [data-testid="stImage"] > div > div > button,
    .logo-wrapper [data-testid="stImage"] * button,
    /* Also target any button that appears on hover over images in the middle column */
    div[data-testid="column"]:nth-of-type(2):hover button,
    div[data-testid="column"]:nth-of-type(2) [data-testid="stImage"]:hover button {
        display: none !important;
        visibility: hidden !important;
        opacity: 0 !important;
        pointer-events: none !important;
        width: 0 !important;
        height: 0 !important;
        padding: 0 !important;
        margin: 0 !important;
        position: absolute !important;
        left: -9999px !important;
    }
    
    /* Hide sidebar completely */
    section[data-testid="stSidebar"],
    div[data-testid="stSidebar"],
    .css-1d391kg,
    [data-testid="stSidebar"] {
        display: none !important;
        visibility: hidden !important;
    }
    
    /* Main container styling - Full width Hub style */
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 3rem;
        max-width: 100%;
        padding-left: 4rem;
        padding-right: 4rem;
        background-color: #ffffff;
    }
    
    @media (max-width: 768px) {
        .main .block-container {
            padding-left: 1.5rem;
            padding-right: 1.5rem;
        }
    }
    
    /* Ensure body has light background */
    body {
        background-color: #f8f9fa;
    }
    
    /* Main app background */
    .main {
        background-color: #f8f9fa;
    }
    
    /* Logo container - perfectly centered */
    .logo-wrapper {
        display: flex;
        justify-content: center;
        align-items: center;
        width: 100%;
        margin: 2rem auto 1.5rem auto;
        padding: 0;
        text-align: center;
    }
    
    /* Force logo image centering */
    .logo-wrapper img,
    .logo-wrapper [data-testid="stImage"] img,
    .logo-wrapper [data-testid="stImage"] {
        max-width: 280px !important;
        width: 280px !important;
        height: auto !important;
        margin: 0 auto !important;
        display: block !important;
        text-align: center !important;
    }
    
    /* Center the image container - More aggressive centering */
    [data-testid="stImage"],
    [data-testid="stImage"] > img,
    [data-testid="stImage"] > div {
        margin: 0 auto !important;
        display: flex !important;
        justify-content: center !important;
        align-items: center !important;
        text-align: center !important;
    }
    
    /* Center columns containing logo */
    .stColumn:has([data-testid="stImage"]),
    div[data-testid="column"]:has([data-testid="stImage"]) {
        display: flex !important;
        justify-content: center !important;
        align-items: center !important;
        text-align: center !important;
    }
    
    /* Ensure middle column centers content - More aggressive */
    div[data-testid="column"]:nth-of-type(2),
    div[data-testid="column"]:nth-of-type(2) > div,
    .stColumn:nth-of-type(2),
    .stColumn:nth-of-type(2) > div {
        display: flex !important;
        justify-content: center !important;
        align-items: center !important;
        text-align: center !important;
        width: 100% !important;
        margin: 0 auto !important;
    }
    
    /* Force center any image in the middle column */
    div[data-testid="column"]:nth-of-type(2) [data-testid="stImage"],
    div[data-testid="column"]:nth-of-type(2) [data-testid="stImage"] > div,
    div[data-testid="column"]:nth-of-type(2) [data-testid="stImage"] img,
    .stColumn:nth-of-type(2) [data-testid="stImage"],
    .stColumn:nth-of-type(2) [data-testid="stImage"] > div,
    .stColumn:nth-of-type(2) [data-testid="stImage"] img {
        margin: 0 auto !important;
        display: block !important;
        text-align: center !important;
        max-width: 280px !important;
        width: 280px !important;
    }
    
    .main-header {
        font-size: 2.5rem;
        font-weight: 600;
        color: #1a1a1a;
        text-align: center;
        margin: 0.5rem 0 0.75rem 0;
        letter-spacing: -0.02em;
        line-height: 1.2;
    }
    
    .sub-header {
        text-align: center;
        color: #6b7280;
        font-size: 1rem;
        margin-bottom: 2.5rem;
        font-weight: 400;
        line-height: 1.5;
        max-width: 100%;
    }
    
    /* Button styling - Hub style */
    .stButton > button,
    .stDownloadButton > button,
    button[data-testid="baseButton-primary"],
    button[data-testid="baseButton-secondary"] {
        background: #274156 !important;
        background-color: #274156 !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        padding: 0.75rem 1.5rem !important;
        font-weight: 500 !important;
        font-size: 0.9375rem !important;
        transition: all 0.2s ease !important;
        box-shadow: 0 1px 2px rgba(0,0,0,0.05) !important;
        width: 100% !important;
    }
    
    .stButton > button:hover,
    .stButton > button:focus,
    .stButton > button:active,
    .stDownloadButton > button:hover,
    .stDownloadButton > button:focus,
    .stDownloadButton > button:active,
    button[data-testid="baseButton-primary"]:hover,
    button[data-testid="baseButton-primary"]:focus,
    button[data-testid="baseButton-primary"]:active,
    button[data-testid="baseButton-secondary"]:hover,
    button[data-testid="baseButton-secondary"]:focus,
    button[data-testid="baseButton-secondary"]:active {
        background: #335169 !important;
        background-color: #335169 !important;
        color: white !important;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1) !important;
    }
    
    /* Section headers */
    h2 {
        color: #111827;
        font-weight: 600;
        margin-top: 2.5rem;
        margin-bottom: 1.25rem;
        font-size: 1.5rem;
        letter-spacing: -0.01em;
    }
    
    h3 {
        color: #111827;
        font-weight: 600;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
        font-size: 1.25rem;
        letter-spacing: -0.01em;
    }
    
    /* Info text styling - Hub style card */
    .info-text {
        background: #ffffff;
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        padding: 2rem;
        margin: 2rem 0;
        line-height: 1.7;
        color: #374151;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        max-width: 100%;
    }
    
    .info-text h3 {
        color: #111827 !important;
        margin-top: 0 !important;
        margin-bottom: 1.25rem !important;
        font-size: 1.25rem;
        font-weight: 600;
    }
    
    .info-text ul {
        color: #374151;
        margin: 0 0 1rem 0;
        padding-left: 1.5rem;
    }
    
    .info-text ul li {
        margin-bottom: 0.75rem;
        line-height: 1.6;
    }
    
    .info-text p {
        color: #374151;
        margin: 0;
    }
    
    .info-text strong {
        color: #111827;
        font-weight: 600;
    }
    
    /* Footer styling */
    .footer {
        text-align: center;
        color: #6b7280;
        padding: 2rem 0;
        margin-top: 4rem;
        border-top: 1px solid #e5e7eb;
        font-size: 0.875rem;
    }
    
    /* Ensure all text is visible */
    .main {
        color: #374151;
    }
    
    .stMarkdown {
        color: #374151;
    }
    
    /* Documentation specific styles */
    .doc-intro {
        background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%);
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        padding: 2rem;
        margin: 2rem 0 3rem 0;
        text-align: center;
    }
    
    .doc-intro h2 {
        color: #111827;
        font-size: 2rem;
        font-weight: 600;
        margin: 0 0 0.75rem 0;
    }
    
    .doc-intro p {
        color: #6b7280;
        font-size: 1.1rem;
        margin: 0;
        line-height: 1.6;
    }
</style>
""", unsafe_allow_html=True)

def main():
    # Add JavaScript to center logo and remove image enlarge buttons
    st.markdown("""
    <script>
    function centerLogo() {
        // Find the middle column (2nd column)
        const middleColumn = document.querySelector('div[data-testid="column"]:nth-of-type(2)');
        if (middleColumn) {
            // Force center alignment
            middleColumn.style.display = 'flex';
            middleColumn.style.justifyContent = 'center';
            middleColumn.style.alignItems = 'center';
            middleColumn.style.textAlign = 'center';
            middleColumn.style.width = '100%';
            middleColumn.style.margin = '0 auto';
            
            // Find image in middle column and center it
            const logoImage = middleColumn.querySelector('[data-testid="stImage"]');
            if (logoImage) {
                logoImage.style.margin = '0 auto';
                logoImage.style.display = 'block';
                logoImage.style.textAlign = 'center';
                
                // Also center any nested divs or images
                const nestedDivs = logoImage.querySelectorAll('div, img');
                nestedDivs.forEach(function(el) {
                    el.style.margin = '0 auto';
                    el.style.display = 'block';
                    el.style.textAlign = 'center';
                });
            }
        }
    }
    
    // Run immediately and on delays
    centerLogo();
    setTimeout(centerLogo, 50);
    setTimeout(centerLogo, 100);
    setTimeout(centerLogo, 200);
    setTimeout(centerLogo, 500);
    
    // Watch for DOM changes
    const centerObserver = new MutationObserver(centerLogo);
    centerObserver.observe(document.body, { childList: true, subtree: true });
    
    function removeLogoButtons() {
        // Only target logo images - check if image is in the middle column (logo position)
        const middleColumn = document.querySelector('div[data-testid="column"]:nth-of-type(2)');
        if (middleColumn) {
            // Find all buttons in the middle column
            const allButtons = middleColumn.querySelectorAll('button');
            allButtons.forEach(function(btn) {
                // Check if button is related to image (has fullscreen/expand attributes or is in image container)
                const isImageButton = btn.getAttribute('title') && btn.getAttribute('title').toLowerCase().includes('fullscreen') ||
                                     btn.getAttribute('aria-label') && btn.getAttribute('aria-label').toLowerCase().includes('fullscreen') ||
                                     btn.getAttribute('aria-label') && btn.getAttribute('aria-label').toLowerCase().includes('expand') ||
                                     btn.closest('[data-testid="stImage"]');
                
                if (isImageButton) {
                    // Aggressively hide and remove
                    btn.style.display = 'none';
                    btn.style.visibility = 'hidden';
                    btn.style.opacity = '0';
                    btn.style.pointerEvents = 'none';
                    btn.style.width = '0';
                    btn.style.height = '0';
                    btn.style.padding = '0';
                    btn.style.margin = '0';
                    btn.style.position = 'absolute';
                    btn.style.left = '-9999px';
                    // Remove from DOM
                    try {
                        btn.remove();
                    } catch(e) {}
                }
            });
            
            // Also specifically target image containers
            const logoImages = middleColumn.querySelectorAll('[data-testid="stImage"]');
            logoImages.forEach(function(imgContainer) {
                const buttons = imgContainer.querySelectorAll('button');
                buttons.forEach(function(btn) {
                    btn.style.display = 'none';
                    btn.style.visibility = 'hidden';
                    btn.style.opacity = '0';
                    btn.style.pointerEvents = 'none';
                    btn.style.width = '0';
                    btn.style.height = '0';
                    btn.style.padding = '0';
                    btn.style.margin = '0';
                    btn.style.position = 'absolute';
                    btn.style.left = '-9999px';
                    try {
                        btn.remove();
                    } catch(e) {}
                });
            });
        }
    }
    
    // Run immediately and more frequently
    removeLogoButtons();
    setTimeout(removeLogoButtons, 50);
    setTimeout(removeLogoButtons, 100);
    setTimeout(removeLogoButtons, 200);
    setTimeout(removeLogoButtons, 500);
    setInterval(removeLogoButtons, 200);
    
    // Watch for new elements with more aggressive settings
    const observer = new MutationObserver(function(mutations) {
        removeLogoButtons();
    });
    observer.observe(document.body, { 
        childList: true, 
        subtree: true,
        attributes: true,
        attributeFilter: ['style', 'class']
    });
    </script>
    """, unsafe_allow_html=True)
    
    # Logo - Centered using columns
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        try:
            st.image("assets/Logo.svg", width=280)
        except:
            st.image("assets/Logo.svg")
    
    # Header
    st.markdown('<h1 class="main-header">Documentation</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Complete guide to using the BigQuery Data Processor</p>', unsafe_allow_html=True)
    
    # Add JavaScript to ensure button hover effect works (including download buttons)
    st.markdown("""
    <script>
    function styleButtons() {
        document.querySelectorAll('.stButton > button, .stDownloadButton > button, button[data-testid="baseButton-primary"], button[data-testid="baseButton-secondary"]').forEach(function(btn) {
            if (!btn.dataset.styled) {
                // Set initial styles
                btn.style.backgroundColor = '#274156';
                btn.style.color = '#ffffff';
                btn.style.border = 'none';
                btn.style.borderRadius = '8px';
                btn.style.transition = 'all 0.2s ease';
                btn.dataset.styled = 'true';
                
                // Add hover event listeners
                btn.addEventListener('mouseenter', function() {
                    this.style.backgroundColor = '#335169';
                    this.style.color = '#ffffff';
                    this.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)';
                });
                btn.addEventListener('mouseleave', function() {
                    this.style.backgroundColor = '#274156';
                    this.style.color = '#ffffff';
                    this.style.boxShadow = '0 1px 2px rgba(0,0,0,0.05)';
                });
            }
        });
    }
    
    // Run immediately and on delays
    styleButtons();
    setTimeout(styleButtons, 100);
    setTimeout(styleButtons, 500);
    
    // Watch for new buttons
    const observer = new MutationObserver(styleButtons);
    observer.observe(document.body, { childList: true, subtree: true });
    </script>
    """, unsafe_allow_html=True)
    
    # Navigation buttons
    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        if st.button("← Back to Main Page", use_container_width=True, key="back_to_main"):
            st.switch_page("app.py")
    with col3:
        if st.button("Schema Guide →", use_container_width=True, key="nav_to_schema_docs"):
            st.switch_page("pages/Schema_Documentation.py")
    
    # Documentation content
    st.markdown("""
    <div class="doc-intro">
        <h2>Getting Started with BigQuery</h2>
        <p>Follow these step-by-step instructions to upload your processed data to Google BigQuery</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Step 1
    st.markdown("""
    <div style="margin: 3rem 0 2rem 0;">
        <h3 style="color: #111827; font-size: 1.75rem; font-weight: 600; margin: 0 0 1.5rem 0; display: flex; align-items: center;">
            <span style="background: #177091; color: #ffffff; width: 40px; height: 40px; border-radius: 50%; text-align: center; line-height: 40px; font-weight: 600; font-size: 1.1rem; margin-right: 1rem; display: inline-block;">1</span>
            Sign in to Google BigQuery
        </h3>
        <div style="color: #374151; line-height: 1.9; font-size: 1.05rem;">
            <ol style="margin: 1rem 0; padding-left: 2rem;">
                <li style="margin-bottom: 1rem;">Sign in to your Google BigQuery account.</li>
                <li style="margin-bottom: 1rem;">Select the project folder where you want to store your data.</li>
            </ol>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Step 2
    st.markdown("""
    <div style="margin: 3rem 0 2rem 0;">
        <h3 style="color: #111827; font-size: 1.75rem; font-weight: 600; margin: 0 0 1.5rem 0; display: flex; align-items: center;">
            <span style="background: #177091; color: #ffffff; width: 40px; height: 40px; border-radius: 50%; text-align: center; line-height: 40px; font-weight: 600; font-size: 1.1rem; margin-right: 1rem; display: inline-block;">2</span>
            Create a Dataset
        </h3>
        <div style="color: #374151; line-height: 1.9; font-size: 1.05rem;">
            <ol style="margin: 1rem 0; padding-left: 2rem;">
                <li style="margin-bottom: 1rem;">Once you are inside the selected folder, you should see three dots <span style="font-size: 1.2em; vertical-align: middle; background-color: #e5e7eb; padding: 0.05rem 0.35rem; border-radius: 4px; display: inline-block;">⋮</span>.</li>
                <li style="margin-bottom: 1rem;">Click on these dots and you will be prompted with multiple options.</li>
                <li style="margin-bottom: 1rem;">As shown in the image below, select the first option which is <strong>Create Dataset</strong>.</li>
            </ol>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Display screenshot for Step 2
    try:
        st.image("assets/Documentation Assests/ss_1.png", width=750, output_format="PNG")
    except:
        st.image("assets/Documentation Assests/ss_1.png", width=750)
    
    # Step 3
    st.markdown("""
    <div style="margin: 3rem 0 2rem 0;">
        <h3 style="color: #111827; font-size: 1.75rem; font-weight: 600; margin: 0 0 1.5rem 0; display: flex; align-items: center;">
            <span style="background: #177091; color: #ffffff; width: 40px; height: 40px; border-radius: 50%; text-align: center; line-height: 40px; font-weight: 600; font-size: 1.1rem; margin-right: 1rem; display: inline-block;">3</span>
            Configure Dataset Settings
        </h3>
        <div style="color: #374151; line-height: 1.9; font-size: 1.05rem;">
            <ol style="margin: 1rem 0; padding-left: 2rem;">
                <li style="margin-bottom: 1rem;">Once the <strong>Create Dataset</strong> option is selected, the create dataset pane will open and you should see the options shown in the image below.</li>
            </ol>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Display screenshot for Step 3
    try:
        st.image("assets/Documentation Assests/ss_2.png", width=750, output_format="PNG")
    except:
        st.image("assets/Documentation Assests/ss_2.png", width=750)
    
    st.markdown("""
    <div style="color: #374151; line-height: 1.9; font-size: 1.05rem; margin-top: 1.5rem;">
        <ol start="2" style="margin: 1rem 0; padding-left: 2rem;">
            <li style="margin-bottom: 1rem;">In the <strong>Dataset ID</strong> field, name your folder you want to place the dataset in. Make sure that only letters, numbers, and underscores are allowed.</li>
            <li style="margin-bottom: 1rem;">In the <strong>Location type</strong>, select region and from the region dropdown, choose <strong>London</strong>. You can also write "london" and you should see <strong>europe-west2 (London)</strong>. Select this option.</li>
            <li style="margin-bottom: 1rem;">Once all these 3 steps are complete, click <strong>Create Dataset</strong> and you should see the newly created dataset folder.</li>
        </ol>
    </div>
    """, unsafe_allow_html=True)
    
    # Step 4
    st.markdown("""
    <div style="margin: 3rem 0 2rem 0;">
        <h3 style="color: #111827; font-size: 1.75rem; font-weight: 600; margin: 0 0 1.5rem 0; display: flex; align-items: center;">
            <span style="background: #177091; color: #ffffff; width: 40px; height: 40px; border-radius: 50%; text-align: center; line-height: 40px; font-weight: 600; font-size: 1.1rem; margin-right: 1rem; display: inline-block;">4</span>
            Create Table and Upload Data
        </h3>
    </div>
    """, unsafe_allow_html=True)
    
    # Display screenshot for Step 4 (right after heading)
    try:
        st.image("assets/Documentation Assests/ss_3.png", width=750, output_format="PNG")
    except:
        st.image("assets/Documentation Assests/ss_3.png", width=750)
    
    st.markdown("""
    <div style="color: #374151; line-height: 1.9; font-size: 1.05rem;">
        <ol style="margin: 1rem 0; padding-left: 2rem;">
            <li style="margin-bottom: 1rem;">Once the new dataset is created, click on the three dots <span style="font-size: 1.2em; vertical-align: middle; background-color: #e5e7eb; padding: 0.05rem 0.35rem; border-radius: 4px; display: inline-block;">⋮</span>, you will be prompted with multiple options. Make sure you select these options carefully as this will affect the data you are uploading.</li>
            <li style="margin-bottom: 1rem;">From these options, select <strong>Create table</strong> option and another pane will open as shown in the image below:</li>
        </ol>
    </div>
    """, unsafe_allow_html=True)
    
    # Display screenshot ss_4_a after point 2
    try:
        st.image("assets/Documentation Assests/ss_4_a.png", width=750, output_format="PNG")
    except:
        st.image("assets/Documentation Assests/ss_4_a.png", width=750)
    
    st.markdown("""
    <div style="color: #374151; line-height: 1.9; font-size: 1.05rem; margin-top: 1.5rem;">
        <ol start="3" style="margin: 1rem 0; padding-left: 2rem;">
            <li style="margin-bottom: 1rem;">The <strong>Create table</strong> panel will appear, displaying a range of options and fields used to configure how your data will be imported into BigQuery. You don't need to fill in every field—only the key ones required to upload your file properly.</li>
            <li style="margin-bottom: 1rem;">In the <strong>Source</strong> section, you will find the dropdown labeled <strong>Create table from</strong>. From the available options, select <strong>Upload</strong>. This tells BigQuery that you will be uploading a file directly from your local system rather than using another source like Google Cloud Storage or a BigQuery table.</li>
            <li style="margin-bottom: 1rem;">Click the <strong>Browse</strong> button to open your file explorer and select the cleaned CSV file you previously prepared.<br><br><em>Note: This is the version of your data that has already been cleaned and formatted using this tool. Once selected, the file will be ready for upload.</em></li>
            <li style="margin-bottom: 1rem;">In the <strong>File format</strong> field, you don't need to make any manual changes. BigQuery automatically detects the file type based on the uploaded file.</li>
            <li style="margin-bottom: 1rem;">In the <strong>Dataset</strong> field, make sure to select the correct dataset folder name where you want the new table to be stored. Datasets in BigQuery act like folders that organise your tables, so double-check that you've chosen the appropriate one for your project.</li>
            <li style="margin-bottom: 1rem;">In the <strong>Table</strong> field, enter a descriptive and clear name for the table/dataset you're creating. This will be the name used to reference the data inside your selected dataset.</li>
        </ol>
    </div>
    """, unsafe_allow_html=True)
    
    # Display screenshot ss_4_a_a after point 7
    try:
        st.image("assets/Documentation Assests/ss_4_a_a.png", width=750, output_format="PNG")
    except:
        st.image("assets/Documentation Assests/ss_4_a_a.png", width=750)
    
    st.markdown("""
    <div style="color: #374151; line-height: 1.9; font-size: 1.05rem; margin-top: 1.5rem;">
        <ol start="8" style="margin: 1rem 0; padding-left: 2rem;">
            <li style="margin-bottom: 1rem;">Under the <strong>Schema</strong> section, locate and switch the toggle labeled <strong>"Edit as text"</strong>. This will open a text field where you need to paste the data schema in JSON format. To get the schema file, download it along with the cleaned CSV file from this tool. The schema file will be named in the following format: <strong>"filename_bq_schema.json"</strong> (where "filename" is the name of your uploaded file). Open this JSON file, copy its entire contents, and paste it into the text field area in BigQuery.</li>
            <li style="margin-bottom: 1rem;">Once you have followed these steps correctly, you just need to follow the last 2 options shown in the image below:</li>
        </ol>
    </div>
    """, unsafe_allow_html=True)
    
    # Display screenshot ss_4_b after point 9
    try:
        st.image("assets/Documentation Assests/ss_4_b.png", width=750, output_format="PNG")
    except:
        st.image("assets/Documentation Assests/ss_4_b.png", width=750)
    
    st.markdown("""
    <div style="color: #374151; line-height: 1.9; font-size: 1.05rem; margin-top: 1.5rem;">
        <ol start="10" style="margin: 1rem 0; padding-left: 2rem;">
            <li style="margin-bottom: 1rem;">Under the <strong>Advanced options</strong> section, you will find an option labeled <strong>"Headers to Skip"</strong>. By default, this is set to 0. Change this value to <strong>1</strong>. This tells BigQuery to skip the column headers (the first row) while parsing the data, ensuring that your headers are not imported as data rows.</li>
            <li style="margin-bottom: 1rem;">Make sure you always select the <strong>Quoted newlines</strong> option while uploading the data. This is available in the available options as shown in the image above.</li>
            <li style="margin-bottom: 1rem;">Once all these options are selected correctly, you can then click <strong>Create table</strong>.</li>
        </ol>
    </div>
    """, unsafe_allow_html=True)
    
    # Step 5
    st.markdown("""
    <div style="margin: 3rem 0 2rem 0;">
        <h3 style="color: #111827; font-size: 1.75rem; font-weight: 600; margin: 0 0 1.5rem 0; display: flex; align-items: center;">
            <span style="background: #177091; color: #ffffff; width: 40px; height: 40px; border-radius: 50%; text-align: center; line-height: 40px; font-weight: 600; font-size: 1.1rem; margin-right: 1rem; display: inline-block;">5</span>
            Appending Data in BigQuery
        </h3>
        <div style="color: #374151; line-height: 1.9; font-size: 1.05rem;">
            <p style="margin-bottom: 1.5rem;">If you are appending data for the first time, you need to run the following SQL query in BigQuery to combine your split tables into a single main table.</p>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown('<p style="margin-bottom: 0.75rem; font-weight: 600; color: #111827; font-size: 1.05rem;">Query to run in BigQuery:</p>', unsafe_allow_html=True)
    
    # Display SQL query in a code block
    sql_query = """CREATE OR REPLACE TABLE `folder.main` AS

SELECT * FROM `folder.split_1`

UNION ALL

SELECT * FROM `folder.split_2`;"""
    
    st.code(sql_query, language='sql')
    
    # Explanation sections - matching rest of documentation style
    st.markdown("""
    <div style="color: #374151; line-height: 1.9; font-size: 1.05rem; margin-top: 1.5rem;">
        <h4 style="color: #111827; font-size: 1.25rem; font-weight: 600; margin: 1.5rem 0 1rem 0;">Explanation of Placeholders</h4>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="color: #374151; line-height: 1.9; font-size: 1.05rem;">
        <ul style="margin: 1rem 0; padding-left: 2rem;">
            <li style="margin-bottom: 1rem;"><strong>folder</strong> - The name of your BigQuery dataset. This is where all your tables are stored.</li>
            <li style="margin-bottom: 1rem;"><strong>main</strong> - The name of the table you are creating. This becomes your combined full dataset that contains all the appended data.</li>
            <li style="margin-bottom: 1rem;"><strong>split_1</strong> - The name of the first table you want to include in the combined dataset.</li>
            <li style="margin-bottom: 1rem;"><strong>split_2</strong> - The name of the second table you want to append directly after split_1.</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="color: #374151; line-height: 1.9; font-size: 1.05rem; margin-top: 1.5rem;">
        <h4 style="color: #111827; font-size: 1.25rem; font-weight: 600; margin: 1.5rem 0 1rem 0;">What the Query Does</h4>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="color: #374151; line-height: 1.9; font-size: 1.05rem;">
        <ol style="margin: 1rem 0; padding-left: 2rem;">
            <li style="margin-bottom: 1rem;"><strong>CREATE OR REPLACE TABLE `folder.main`</strong> - Creates a new table called "main" in your dataset. If a table with that name already exists, it completely replaces it with the new data.</li>
            <li style="margin-bottom: 1rem;"><strong>SELECT * FROM `folder.split_1`</strong> - Takes all columns and rows from the first table (split_1).</li>
            <li style="margin-bottom: 1rem;"><strong>UNION ALL SELECT * FROM `folder.split_2`</strong> - Adds all rows from the second table (split_2) directly underneath the rows from split_1, without removing any duplicates.</li>
        </ol>
    </div>
    """, unsafe_allow_html=True)
    
    # Why UNION ALL is Used - with translucent blue background
    st.markdown("""
    <div style="background: rgba(23, 112, 145, 0.1); border-left: 4px solid #177091; border-radius: 4px; padding: 1rem; margin: 1.5rem 0;">
        <p style="margin: 0; color: #374151; line-height: 1.9; font-size: 1.05rem;"><strong>Why UNION ALL is Used:</strong> We use UNION ALL because we want to append the data exactly as it is, without removing duplicates or merging rows. This keeps all rows from both split_1 and split_2 in the final combined table.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Step-by-step instructions
    st.markdown("""
    <div style="color: #374151; line-height: 1.9; font-size: 1.05rem; margin: 1.5rem 0;">
        <ol style="margin: 1rem 0; padding-left: 2rem;">
            <li style="margin-bottom: 1rem;">Select either your dataset folder or any table inside it. A pane will open where you can see the plus sign (+) on the top as shown in the image below:</li>
        </ol>
    </div>
    """, unsafe_allow_html=True)
    
    # Display screenshot ss_5_a
    try:
        st.image("assets/Documentation Assests/ss_5_a.png", width=750, output_format="PNG")
    except:
        st.image("assets/Documentation Assests/ss_5_a.png", width=750)
    
    st.markdown("""
    <div style="color: #374151; line-height: 1.9; font-size: 1.05rem; margin-top: 1.5rem;">
        <ol start="2" style="margin: 1rem 0; padding-left: 2rem;">
            <li style="margin-bottom: 1rem;">Once you select the plus (+) option, the SQL editor will open. See the image below:</li>
        </ol>
    </div>
    """, unsafe_allow_html=True)
    
    # Display screenshot ss_5_b
    try:
        st.image("assets/Documentation Assests/ss_5_b.png", width=750, output_format="PNG")
    except:
        st.image("assets/Documentation Assests/ss_5_b.png", width=750)
    
    st.markdown("""
    <div style="color: #374151; line-height: 1.9; font-size: 1.05rem; margin-top: 1.5rem;">
        <ol start="3" style="margin: 1rem 0; padding-left: 2rem;">
            <li style="margin-bottom: 1rem;">Once you paste the query inside the editor, you should be able to run the query by clicking on the <strong>Run</strong> button.</li>
        </ol>
    </div>
    """, unsafe_allow_html=True)
    
    # Appending Additional Data Section
    st.markdown("""
    <div style="color: #374151; line-height: 1.9; font-size: 1.05rem; margin: 2.5rem 0 1.5rem 0;">
        <h4 style="color: #111827; font-size: 1.25rem; font-weight: 600; margin: 0 0 1rem 0;">Appending Additional Data</h4>
        <p style="margin-bottom: 1.5rem;">After you have successfully run the initial query above and created your main table, you may need to append new or updated data to your existing table. For example, if you have a new data file that contains updated or additional records that you want to add to your existing main table, you can use the following query.</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown('<p style="margin-bottom: 0.75rem; font-weight: 600; color: #111827; font-size: 1.05rem;">Query to append additional data:</p>', unsafe_allow_html=True)
    
    # Display INSERT query in a code block
    insert_query = """INSERT INTO `folder.main`

SELECT * FROM `folder.new_data`;"""
    
    st.code(insert_query, language='sql')
    
    st.markdown("""
    <div style="color: #374151; line-height: 1.9; font-size: 1.05rem; margin-top: 1.5rem;">
        <p style="margin-bottom: 1rem;"><strong>Important:</strong> This query should only be run <strong>after</strong> you have successfully executed the initial <code>CREATE OR REPLACE TABLE</code> query. The initial query creates the main table structure, and this <code>INSERT INTO</code> query adds additional rows to the existing table without replacing it.</p>
        <p style="margin-bottom: 1rem;">You can use this query whenever you have new or updated data to append to your main table. Simply replace <code>new_data</code> with the name of your new data table. Each time you run this query, the new data will be appended to the bottom of your main table, preserving all existing data.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Download all images button at the end
    st.markdown("---")
    st.markdown('<h3 style="color: #111827; font-size: 1.5rem; font-weight: 600; margin: 2rem 0 1rem 0;">Download Documentation Images</h3>', unsafe_allow_html=True)
    st.markdown('<p style="color: #6b7280; margin-bottom: 1.5rem;">Download all screenshots used in this documentation for offline viewing.</p>', unsafe_allow_html=True)
    
    # Function to create zip of all documentation images
    def create_docs_images_zip():
        zip_buffer = io.BytesIO()
        image_files = [
            "assets/Documentation Assests/ss_1.png",
            "assets/Documentation Assests/ss_2.png",
            "assets/Documentation Assests/ss_3.png",
            "assets/Documentation Assests/ss_4_a.png",
            "assets/Documentation Assests/ss_4_a_a.png",
            "assets/Documentation Assests/ss_4_b.png",
            "assets/Documentation Assests/ss_5_a.png",
            "assets/Documentation Assests/ss_5_b.png"
        ]
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for img_path in image_files:
                if Path(img_path).exists():
                    zip_file.write(img_path, Path(img_path).name)
        
        zip_buffer.seek(0)
        return zip_buffer.getvalue()
    
    zip_data = create_docs_images_zip()
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        st.download_button(
            label="Download All Images",
            data=zip_data,
            file_name="documentation_images.zip",
            mime="application/zip",
            help="Download all screenshots used in this documentation",
            use_container_width=True,
            key="download_docs_images"
        )
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div class="footer">
        <p style="font-size: 1rem; margin-bottom: 0.25rem;"><strong>BigQuery Data Processor</strong></p>
        <p style="font-size: 0.75rem; color: #9ca3af; margin-bottom: 0.5rem;">Powered by dashworx</p>
        <p style="font-size: 0.9rem; color: #888; margin-bottom: 0.75rem;">Transform your data into BigQuery-ready format | Built with Streamlit</p>
        <p style="font-size: 0.85rem; color: #6b7280; margin-top: 1rem; padding-top: 1rem; border-top: 1px solid #e5e7eb;">
            <strong>🔒 Privacy & Security:</strong> Your data is processed locally and never stored on our servers. All files are processed in temporary memory and automatically deleted after your session ends.
        </p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()

