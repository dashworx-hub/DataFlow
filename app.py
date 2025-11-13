import streamlit as st
import pandas as pd
import json
import tempfile
import zipfile
from pathlib import Path
import io
import sys
import os

# Import the processing functions from main.py
from main import (
    process_xlsx, process_csv, 
    bq_schema_from_df, format_dates_for_csv,
    write_clean_csv, find_unbalanced_quote_lines
)

# ============================================================================
# MAINTENANCE MODE CHECK
# ============================================================================
def is_maintenance_mode():
    """
    Check if maintenance mode is enabled via environment variable or Streamlit secret.
    Returns True if maintenance mode is active, False otherwise.
    """
    # Check environment variable first
    env_flag = os.getenv("MAINTENANCE_MODE", "").lower()
    if env_flag in ("true", "1", "yes", "on"):
        return True
    
    # Check Streamlit secrets as fallback
    try:
        secret_flag = st.secrets.get("MAINTENANCE_MODE", "").lower()
        if secret_flag in ("true", "1", "yes", "on"):
            return True
    except (AttributeError, KeyError, FileNotFoundError):
        # Secrets not configured, which is fine
        pass
    
    return False

# ============================================================================
# SHARED UI ELEMENTS
# ============================================================================
def render_shared_css():
    """Render the shared CSS styling used by both main app and maintenance page."""
    st.markdown("""
    <style>
        /* Hide default Streamlit elements */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        
        /* Hide image enlarge/fullscreen button - Only for logo images */
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
        
        /* Center the image container */
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
        
        /* Ensure middle column centers content */
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
        .stDownloadButton > button {
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
        .stDownloadButton > button:active {
            background: #335169 !important;
            background-color: #335169 !important;
            color: white !important;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1) !important;
        }
        
        /* Maintenance page specific styles */
        .maintenance-container {
            background: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 16px;
            padding: 4rem 3rem;
            margin: 3rem auto;
            max-width: 800px;
            text-align: center;
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        }
        
        .maintenance-title {
            font-size: 2rem;
            font-weight: 600;
            color: #111827;
            margin-bottom: 1.5rem;
        }
        
        .maintenance-message {
            font-size: 1.1rem;
            color: #6b7280;
            line-height: 1.7;
            margin-bottom: 2rem;
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
        
        /* Feature cards */
        .feature-card {
            background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%);
            border: 1px solid #e9ecef;
            border-radius: 12px;
            padding: 1.5rem;
            margin: 1rem 0;
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
            transition: transform 0.2s, box-shadow 0.2s;
        }
        
        .feature-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        }
        
        /* Upload area styling */
        .upload-section {
            background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%);
            border: 2px dashed #177091;
            border-radius: 16px;
            padding: 3rem 2rem;
            text-align: center;
            margin: 2rem 0;
            transition: all 0.3s ease;
        }
        
        .upload-section:hover {
            border-color: #13718F;
            background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%);
        }
        
        /* Metric cards */
        .metric-container {
            background: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 10px;
            padding: 1.5rem;
            text-align: center;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        }
        
        /* Success/Warning/Error boxes */
        .success-box {
            background: #ecfdf5;
            border: 1px solid #10b981;
            border-left: 4px solid #10b981;
            border-radius: 10px;
            padding: 1.25rem;
            margin: 1rem 0;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        }
        
        .warning-box {
            background: #fffbeb;
            border: 1px solid #f59e0b;
            border-left: 4px solid #f59e0b;
            border-radius: 10px;
            padding: 1.25rem;
            margin: 1rem 0;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        }
        
        .error-box {
            background: #fef2f2;
            border: 1px solid #ef4444;
            border-left: 4px solid #ef4444;
            border-radius: 10px;
            padding: 1.25rem;
            margin: 1rem 0;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
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
        
        /* Info text styling */
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
        
        /* File info styling */
        .file-info {
            background: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 10px;
            padding: 1.25rem 1.5rem;
            margin: 1rem 0 1.5rem 0;
            color: #374151;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
            font-size: 0.9375rem;
            line-height: 1.6;
            width: 100%;
            display: block;
        }
        
        .file-info strong {
            color: #111827;
            font-weight: 600;
        }
        
        /* Alert badge styling */
        .upload-alert {
            display: inline-flex;
            align-items: center;
            background: #fef3c7;
            border: 1px solid #f59e0b;
            border-radius: 6px;
            padding: 0.375rem 0.75rem;
            margin-left: 0.75rem;
            font-size: 0.875rem;
            color: #92400e;
            font-weight: 500;
        }
        
        .upload-alert::before {
            content: "ℹ️";
            margin-right: 0.375rem;
        }
        
        /* Fix text color in success/warning/error boxes */
        .success-box, .warning-box, .error-box {
            color: #374151;
            border-radius: 10px;
            border: 1px solid;
        }
        
        .success-box {
            border-color: #10b981;
        }
        
        .warning-box {
            border-color: #f59e0b;
        }
        
        .error-box {
            border-color: #ef4444;
        }
        
        .success-box strong, .warning-box strong, .error-box strong {
            color: #111827;
        }
        
        /* Metric container text */
        .metric-container {
            color: #374151;
            border: 1px solid #e5e7eb;
        }
        
        /* Download button styling */
        .stDownloadButton > button,
        .stDownloadButton button,
        button[data-testid="baseButton-secondary"],
        button[data-testid="baseButton-secondary"]:hover,
        button[data-testid="baseButton-secondary"]:focus,
        button[data-testid="baseButton-secondary"]:active,
        div[data-testid="stDownloadButton"] > button,
        div[data-testid="stDownloadButton"] button {
            background: #274156 !important;
            background-color: #274156 !important;
            color: #ffffff !important;
            border: none !important;
            border-color: transparent !important;
            border-radius: 8px !important;
            padding: 0.75rem 1.5rem !important;
            font-weight: 500 !important;
            font-size: 0.9375rem !important;
            transition: all 0.2s ease !important;
            box-shadow: 0 1px 2px rgba(0,0,0,0.05) !important;
            width: 100% !important;
        }
        
        .stDownloadButton > button:hover,
        .stDownloadButton button:hover,
        button[data-testid="baseButton-secondary"]:hover,
        div[data-testid="stDownloadButton"] > button:hover,
        div[data-testid="stDownloadButton"] button:hover {
            background: #335169 !important;
            background-color: #335169 !important;
            color: #ffffff !important;
            border: none !important;
            border-color: transparent !important;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1) !important;
        }
        
        /* File uploader styling */
        [data-testid="stFileUploader"] {
            color: #000000 !important;
            background: transparent !important;
            background-color: transparent !important;
        }
        
        [data-testid="stFileUploader"] > div {
            background: transparent !important;
            background-color: transparent !important;
            border-color: #d1d5db !important;
        }
        
        [data-testid="stFileUploader"] > div > div {
            background: transparent !important;
            background-color: transparent !important;
            border-color: #d1d5db !important;
        }
        
        [data-testid="stFileUploader"] p,
        [data-testid="stFileUploader"] span,
        [data-testid="stFileUploader"] div,
        [data-testid="stFileUploader"] *:not(button) {
            color: #000000 !important;
        }
        
        [data-testid="stFileUploader"] > div:hover {
            background: transparent !important;
            background-color: transparent !important;
            border-color: #d1d5db !important;
        }
        
        [data-testid="stFileUploader"] > div > div:hover {
            background: transparent !important;
            background-color: transparent !important;
            border-color: #d1d5db !important;
        }
        
        [data-testid="stFileUploader"] button,
        [data-testid="stFileUploader"] > div > div > button,
        [data-testid="stFileUploader"] button[type="button"] {
            background: #274156 !important;
            background-color: #274156 !important;
            border-color: #274156 !important;
            border: 1px solid #274156 !important;
            color: #ffffff !important;
            border-radius: 8px !important;
        }
        
        [data-testid="stFileUploader"] button:hover,
        [data-testid="stFileUploader"] button:focus,
        [data-testid="stFileUploader"] button:active,
        [data-testid="stFileUploader"] > div > div > button:hover,
        [data-testid="stFileUploader"] > div > div > button:focus,
        [data-testid="stFileUploader"] > div > div > button:active,
        [data-testid="stFileUploader"] button[type="button"]:hover,
        [data-testid="stFileUploader"] button[type="button"]:focus {
            background: #335169 !important;
            background-color: #335169 !important;
            border-color: #335169 !important;
            border: 1px solid #335169 !important;
            color: #ffffff !important;
            outline: none !important;
        }
    </style>
    """, unsafe_allow_html=True)

def render_shared_logo_script():
    """Render the shared JavaScript for logo centering and button removal."""
    st.markdown("""
    <script>
    function centerLogo() {
        const middleColumn = document.querySelector('div[data-testid="column"]:nth-of-type(2)');
        if (middleColumn) {
            middleColumn.style.display = 'flex';
            middleColumn.style.justifyContent = 'center';
            middleColumn.style.alignItems = 'center';
            middleColumn.style.textAlign = 'center';
            middleColumn.style.width = '100%';
            middleColumn.style.margin = '0 auto';
            
            const logoImage = middleColumn.querySelector('[data-testid="stImage"]');
            if (logoImage) {
                logoImage.style.margin = '0 auto';
                logoImage.style.display = 'block';
                logoImage.style.textAlign = 'center';
                
                const nestedDivs = logoImage.querySelectorAll('div, img');
                nestedDivs.forEach(function(el) {
                    el.style.margin = '0 auto';
                    el.style.display = 'block';
                    el.style.textAlign = 'center';
                });
            }
        }
    }
    
    centerLogo();
    setTimeout(centerLogo, 50);
    setTimeout(centerLogo, 100);
    setTimeout(centerLogo, 200);
    setTimeout(centerLogo, 500);
    
    const centerObserver = new MutationObserver(centerLogo);
    centerObserver.observe(document.body, { childList: true, subtree: true });
    
    function removeLogoButtons() {
        const middleColumn = document.querySelector('div[data-testid="column"]:nth-of-type(2)');
        if (middleColumn) {
            const allButtons = middleColumn.querySelectorAll('button');
            allButtons.forEach(function(btn) {
                const isImageButton = btn.getAttribute('title') && btn.getAttribute('title').toLowerCase().includes('fullscreen') ||
                                     btn.getAttribute('aria-label') && btn.getAttribute('aria-label').toLowerCase().includes('fullscreen') ||
                                     btn.getAttribute('aria-label') && btn.getAttribute('aria-label').toLowerCase().includes('expand') ||
                                     btn.closest('[data-testid="stImage"]');
                
                if (isImageButton) {
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
                }
            });
            
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
    
    removeLogoButtons();
    setTimeout(removeLogoButtons, 50);
    setTimeout(removeLogoButtons, 100);
    setTimeout(removeLogoButtons, 200);
    setTimeout(removeLogoButtons, 500);
    setInterval(removeLogoButtons, 200);
    
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

def render_shared_logo():
    """Render the shared logo in centered columns."""
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        try:
            st.image("assets/Logo.svg", width=280)
        except:
            st.image("assets/Logo.svg")

def render_shared_footer():
    """Render the shared footer."""
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

def render_shared_layout():
    """Render all shared layout elements (CSS, logo script, logo, and footer)."""
    render_shared_css()
    render_shared_logo_script()
    render_shared_logo()

# ============================================================================
# MAINTENANCE PAGE
# ============================================================================
def render_maintenance_page():
    """
    Render the maintenance page using shared layout elements.
    This function blocks further execution after rendering.
    """
    # Page configuration
    st.set_page_config(
        page_title="Maintenance - BigQuery Data Processor",
        page_icon="assets/web/icons8-hub-pulsar-gradient-32.png",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    # Render shared layout elements
    render_shared_layout()
    
    # Maintenance content
    st.markdown('<h1 class="main-header">BigQuery Data Processor</h1>', unsafe_allow_html=True)
    
    # Maintenance message container with image
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        try:
            st.image("assets/Maintenance.svg", width=200)
        except:
            pass
    
    st.markdown("""
    <div class="maintenance-container">
        <div class="maintenance-title">🔧 Under Maintenance</div>
        <div class="maintenance-message">
            We're currently performing scheduled maintenance to improve your experience.<br>
            Please check back soon. We apologize for any inconvenience.
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Render footer
    render_shared_footer()
    
    # Stop execution - nothing from main app should run
    st.stop()

# ============================================================================
# MAIN APP FUNCTIONALITY
# ============================================================================
def create_download_zip(temp_dir):
    """Create a zip file with all output files"""
    zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for file_path in Path(temp_dir).rglob('*'):
            if file_path.is_file():
                zip_file.write(file_path, file_path.relative_to(temp_dir))
    
    zip_buffer.seek(0)
    return zip_buffer.getvalue()

def create_csv_zip(temp_dir):
    """Create a zip file with only CSV files"""
    zip_buffer = io.BytesIO()
    output_dir = Path(temp_dir)
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        csv_files = list(output_dir.glob("*.csv"))
        for csv_file in csv_files:
            zip_file.write(csv_file, csv_file.name)
    
    zip_buffer.seek(0)
    return zip_buffer.getvalue()

def create_schema_zip(temp_dir):
    """Create a zip file with only schema JSON files"""
    zip_buffer = io.BytesIO()
    output_dir = Path(temp_dir)
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        schema_files = list(output_dir.glob("*_bq_schema.json"))
        for schema_file in schema_files:
            zip_file.write(schema_file, schema_file.name)
    
    zip_buffer.seek(0)
    return zip_buffer.getvalue()

def create_summary_zip(temp_dir):
    """Create a zip file with only summary TXT files"""
    zip_buffer = io.BytesIO()
    output_dir = Path(temp_dir)
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        summary_files = list(output_dir.glob("*_summary.txt"))
        for summary_file in summary_files:
            zip_file.write(summary_file, summary_file.name)
    
    zip_buffer.seek(0)
    return zip_buffer.getvalue()

def display_processing_results(temp_dir):
    """Display processing results in a user-friendly format"""
    
    output_dir = Path(temp_dir)
    if not output_dir.exists():
        st.markdown("""
        <div class="error-box">
            <strong>❌ Error</strong><br>
            No output directory found
        </div>
        """, unsafe_allow_html=True)
        return
    
    csv_files = list(output_dir.glob("*.csv"))
    schema_files = list(output_dir.glob("*_bq_schema.json"))
    summary_files = list(output_dir.glob("*_summary.txt"))
    
    st.markdown('<h2>Processing Summary</h2>', unsafe_allow_html=True)
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="metric-container">
            <div style="font-size: 2.5rem; font-weight: 700; color: #177091;">{}</div>
            <div style="color: #666; margin-top: 0.5rem;">Processed Sheets</div>
        </div>
        """.format(len(csv_files)), unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="metric-container">
            <div style="font-size: 2.5rem; font-weight: 700; color: #177091;">{}</div>
            <div style="color: #666; margin-top: 0.5rem;">Schema Files</div>
        </div>
        """.format(len(schema_files)), unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="metric-container">
            <div style="font-size: 2.5rem; font-weight: 700; color: #177091;">{}</div>
            <div style="color: #666; margin-top: 0.5rem;">Summary Files</div>
        </div>
        """.format(len(summary_files)), unsafe_allow_html=True)
    
    if not csv_files:
        st.markdown("""
        <div class="warning-box">
            <strong>⚠️ Warning</strong><br>
            No CSV files were generated during processing.
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown('<h2>Processed Files</h2>', unsafe_allow_html=True)
    
    for csv_file in csv_files:
        sheet_name = csv_file.stem
        schema_file = output_dir / f"{sheet_name}_bq_schema.json"
        summary_file = output_dir / f"{sheet_name}_summary.txt"
        
        with st.expander(f"📋 Sheet: {sheet_name}", expanded=len(csv_files) == 1):
            
            if schema_file.exists():
                with open(schema_file, 'r', encoding='utf-8') as f:
                    schema = json.load(f)
                
                st.markdown('<h3>🗂️ BigQuery Schema</h3>', unsafe_allow_html=True)
                st.json(schema)
            
            if summary_file.exists():
                with open(summary_file, 'r', encoding='utf-8') as f:
                    summary_content = f.read()
                
                st.markdown('<h3>📝 Column Summary</h3>', unsafe_allow_html=True)
                st.markdown(f"""
                <div style="background: #ffffff; border: 1px solid #e9ecef; padding: 1rem; border-radius: 8px; font-family: monospace; white-space: pre-wrap; color: #2c3e50;">
                {summary_content}
                </div>
                """, unsafe_allow_html=True)
            
            try:
                df = pd.read_csv(csv_file, nrows=5)
                st.markdown('<h3>📊 Data Preview (First 5 rows)</h3>', unsafe_allow_html=True)
                st.dataframe(df, use_container_width=True)
            except Exception as e:
                st.markdown(f"""
                <div class="warning-box">
                    <strong>⚠️ Preview Unavailable</strong><br>
                    Could not preview CSV: {str(e)}
                </div>
                """, unsafe_allow_html=True)

def run_main_app():
    """
    Main application functionality.
    This function contains all the actual app logic.
    """
    # Page configuration
    st.set_page_config(
        page_title="BigQuery Data Processor",
        page_icon="assets/web/icons8-hub-pulsar-gradient-32.png",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    # Render shared layout elements
    render_shared_layout()
    
    # Header
    st.markdown('<h1 class="main-header">BigQuery Data Processor</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Transform your Excel and CSV files into BigQuery-ready format with intelligent data processing</p>', unsafe_allow_html=True)
    
    # Navigation button to Documentation page
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("View Documentation", use_container_width=True, key="nav_to_docs"):
            st.switch_page("pages/Documentation.py")
    
    # Features section
    st.markdown("""
    <div class="info-text">
        <h3>✨ Key Features</h3>
        <ul>
            <li><strong>Smart Data Type Inference</strong> - Automatically detects STRING, INT64, FLOAT64, BOOL, DATE, and TIMESTAMP types</li>
            <li><strong>Automatic Column Sanitization</strong> - Converts column names to BigQuery-compatible format</li>
            <li><strong>Data Cleaning & Normalization</strong> - Handles missing values, formats dates, and cleans data</li>
            <li><strong>BigQuery Schema Generation</strong> - Generates ready-to-use JSON schema files</li>
            <li><strong>Clean CSV Output</strong> - Produces properly formatted CSV files optimized for BigQuery</li>
        </ul>
        <p style="margin-top: 1.25rem; margin-bottom: 0; padding-top: 1.25rem; border-top: 1px solid #e5e7eb;"><strong>💡 Smart Logic:</strong> Columns containing any letters (A-Z) are automatically detected as STRING type. Simply upload your file and click "Process File" - no configuration needed!</p>
    </div>
    """, unsafe_allow_html=True)
    
    # File upload section
    st.markdown("---")
    st.markdown('<h2 style="display: inline-flex; align-items: center;">Upload Your Data File <span class="upload-alert">Max upload size: 200MB</span></h2>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader(
        "Choose an Excel (.xlsx, .xls) or CSV file",
        type=['xlsx', 'xls', 'csv'],
        help="Maximum file size: 200MB",
        label_visibility="collapsed"
    )
    
    if uploaded_file is not None:
        file_size_mb = len(uploaded_file.getvalue()) / (1024 * 1024)
        
        if file_size_mb > 200:
            st.markdown(f"""
            <div class="error-box">
                <strong>❌ File Size Error</strong><br>
                File size ({file_size_mb:.1f}MB) exceeds the 200MB limit. Please upload a smaller file.
            </div>
            """, unsafe_allow_html=True)
            return
        
        st.markdown(f"""
        <div class="file-info">
            <strong>📄 File:</strong> {uploaded_file.name}<br>
            <strong>📊 Size:</strong> {file_size_mb:.2f} MB<br>
            <strong>✅ Status:</strong> Ready to process
        </div>
        """, unsafe_allow_html=True)
        
        process_btn = st.button("Process File", type="primary", use_container_width=True)
        
        if process_btn:
            with st.spinner("Processing your file..."):
                with tempfile.TemporaryDirectory() as temp_dir:
                    temp_dir = Path(temp_dir)
                    
                    input_path = temp_dir / uploaded_file.name
                    with open(input_path, 'wb') as f:
                        f.write(uploaded_file.getvalue())
                    
                    output_dir = temp_dir / "output"
                    output_dir.mkdir()
                    
                    file_ext = input_path.suffix.lower()
                    
                    try:
                        if file_ext in {'.xlsx', '.xlsm', '.xls'}:
                            process_xlsx(input_path, output_dir)
                        elif file_ext == '.csv':
                            process_csv(input_path, output_dir)
                        else:
                            st.error("Unsupported file type. Please upload .xlsx, .xls, or .csv files.")
                            return
                        
                        st.session_state['output_files'] = {}
                        for file_path in output_dir.rglob('*'):
                            if file_path.is_file():
                                relative_path = file_path.relative_to(output_dir)
                                with open(file_path, 'rb') as f:
                                    st.session_state['output_files'][str(relative_path)] = f.read()
                        
                        st.session_state['processed'] = True
                        st.session_state['uploaded_file_name'] = uploaded_file.name
                    
                    except Exception as e:
                        st.markdown(f"""
                        <div class="error-box">
                            <strong>❌ Processing Failed</strong><br>
                            {str(e)}
                        </div>
                        """, unsafe_allow_html=True)
                        st.exception(e)
                        st.session_state['processed'] = False
                        
        if st.session_state.get('processed', False):
            with tempfile.TemporaryDirectory() as display_temp_dir:
                display_temp_dir = Path(display_temp_dir)
                output_dir = display_temp_dir / "output"
                output_dir.mkdir()
                
                for file_path_str, file_content in st.session_state.get('output_files', {}).items():
                    file_path = output_dir / file_path_str
                    file_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(file_path, 'wb') as f:
                        f.write(file_content)
                
                st.markdown("""
                <div class="success-box">
                    <strong>✅ Processing Completed Successfully!</strong><br>
                    Your data has been processed and is now BigQuery-ready.
                </div>
                """, unsafe_allow_html=True)
                
                display_processing_results(output_dir)
                
                st.markdown("---")
                st.markdown('<h2>Download Results</h2>', unsafe_allow_html=True)
                
                base_name = st.session_state.get('uploaded_file_name', 'processed').split('.')[0]
                
                with tempfile.TemporaryDirectory() as zip_temp_dir:
                    zip_temp_dir = Path(zip_temp_dir)
                    zip_output_dir = zip_temp_dir / "output"
                    zip_output_dir.mkdir()
                    
                    for file_path_str, file_content in st.session_state.get('output_files', {}).items():
                        file_path = zip_output_dir / file_path_str
                        file_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(file_path, 'wb') as f:
                            f.write(file_content)
                    
                    csv_zip_data = create_csv_zip(zip_output_dir)
                    schema_zip_data = create_schema_zip(zip_output_dir)
                    summary_zip_data = create_summary_zip(zip_output_dir)
                    all_zip_data = create_download_zip(zip_output_dir)
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.download_button(
                        label="Download CSV Files",
                        data=csv_zip_data,
                        file_name=f"{base_name}_csv_files.zip",
                        mime="application/zip",
                        help="Download all processed CSV files",
                        use_container_width=True
                    )
                
                with col2:
                    st.download_button(
                        label="Download Schemas",
                        data=schema_zip_data,
                        file_name=f"{base_name}_schemas.zip",
                        mime="application/zip",
                        help="Download all BigQuery schema JSON files",
                        use_container_width=True
                    )
                
                with col3:
                    st.download_button(
                        label="Download Summaries",
                        data=summary_zip_data,
                        file_name=f"{base_name}_summaries.zip",
                        mime="application/zip",
                        help="Download all summary text files",
                        use_container_width=True
                    )
                
                with col4:
                    st.download_button(
                        label="Download All (ZIP)",
                        data=all_zip_data,
                        file_name=f"{base_name}_all_files.zip",
                        mime="application/zip",
                        help="Download all processed files including CSV, schema, and summary files",
                        use_container_width=True
                    )
                
                st.markdown('<h3>Individual Files</h3>', unsafe_allow_html=True)
                
                output_files = [f for f in output_dir.glob("*") if f.is_file()]
                
                files_by_type = {
                    '.csv': [],
                    '.xlsx': [],
                    '.xls': [],
                    '.json': [],
                    '.txt': [],
                    'other': []
                }
                
                for file_path in output_files:
                    ext = file_path.suffix.lower()
                    if ext in files_by_type:
                        files_by_type[ext].append(file_path)
                    else:
                        files_by_type['other'].append(file_path)
                
                for file_type in files_by_type:
                    files_by_type[file_type].sort(key=lambda x: x.name)
                
                file_type_info = {
                    '.csv': {'icon_path': 'assets/csv.png', 'name': 'CSV Files', 'mime': 'text/csv'},
                    '.xlsx': {'icon_path': 'assets/xlsx.png', 'name': 'Excel Files', 'mime': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'},
                    '.xls': {'icon_path': 'assets/xlsx.png', 'name': 'Excel Files', 'mime': 'application/vnd.ms-excel'},
                    '.json': {'icon_path': None, 'icon': '📋', 'name': 'JSON Files', 'mime': 'application/json'},
                    '.txt': {'icon_path': None, 'icon': '📝', 'name': 'Text Files', 'mime': 'text/plain'},
                    'other': {'icon_path': None, 'icon': '📄', 'name': 'Other Files', 'mime': 'application/octet-stream'}
                }
                
                for file_ext, files in files_by_type.items():
                    if files:
                        st.markdown(f'<h4 style="margin-top: 1.5rem; margin-bottom: 0.75rem; color: #177091; font-size: 1.1rem;">{file_type_info[file_ext]["name"]}</h4>', unsafe_allow_html=True)
                        
                        num_cols = min(4, len(files))
                        cols = st.columns(num_cols, gap="small")
                        
                        for idx, file_path in enumerate(files):
                            relative_path_str = str(file_path.relative_to(output_dir))
                            file_data = st.session_state.get('output_files', {}).get(relative_path_str, b'')
                            
                            if file_data:
                                with cols[idx % len(cols)]:
                                    display_name = file_path.name
                                    if len(display_name) > 25:
                                        display_name = display_name[:22] + "..."
                                    
                                    st.download_button(
                                        label=display_name,
                                        data=file_data,
                                        file_name=file_path.name,
                                        mime=file_type_info[file_ext]["mime"],
                                        key=f"file_{file_path.name}_{idx}",
                                        use_container_width=True
                                    )
    
    # Footer
    render_shared_footer()

# ============================================================================
# MAIN ENTRY POINT - CONDITIONAL SWITCH
# ============================================================================
if __name__ == "__main__":
    # Check maintenance mode first - this is the critical switch
    if is_maintenance_mode():
        render_maintenance_page()
        # render_maintenance_page() calls st.stop(), so nothing below runs
    else:
        run_main_app()
