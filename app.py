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

# Page configuration
st.set_page_config(
    page_title="BigQuery Data Processor",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 0.375rem;
        padding: 1rem;
        margin: 1rem 0;
    }
    .warning-box {
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        border-radius: 0.375rem;
        padding: 1rem;
        margin: 1rem 0;
    }
    .error-box {
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        border-radius: 0.375rem;
        padding: 1rem;
        margin: 1rem 0;
    }
    .metric-card {
        background-color: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 0.375rem;
        padding: 1rem;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

def create_download_zip(temp_dir):
    """Create a zip file with all output files"""
    zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        # Add all files from temp directory
        for file_path in Path(temp_dir).rglob('*'):
            if file_path.is_file():
                zip_file.write(file_path, file_path.relative_to(temp_dir))
    
    zip_buffer.seek(0)
    return zip_buffer.getvalue()

def display_processing_results(temp_dir):
    """Display processing results in a user-friendly format"""
    
    output_dir = Path(temp_dir)
    if not output_dir.exists():
        st.error("No output directory found")
        return
    
    # Get all output files
    csv_files = list(output_dir.glob("*.csv"))
    schema_files = list(output_dir.glob("*_bq_schema.json"))
    summary_files = list(output_dir.glob("*_summary.txt"))
    
    # Display metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Processed Sheets", len(csv_files))
    
    with col2:
        st.metric("Schema Files", len(schema_files))
    
    with col3:
        st.metric("Summary Files", len(summary_files))
    
    # Status message
    if csv_files:
        st.success("🎉 Processing completed! Your data is BigQuery-ready.")
    else:
        st.warning("⚠️ No CSV files were generated.")
    
    # Display results for each processed sheet
    st.subheader("📊 Processed Files")
    
    for csv_file in csv_files:
        sheet_name = csv_file.stem
        schema_file = output_dir / f"{sheet_name}_bq_schema.json"
        summary_file = output_dir / f"{sheet_name}_summary.txt"
        
        with st.expander(f"Sheet: {sheet_name}", expanded=len(csv_files) == 1):
            
            # Display schema if available
            if schema_file.exists():
                with open(schema_file, 'r', encoding='utf-8') as f:
                    schema = json.load(f)
                
                st.subheader("🗂️ BigQuery Schema")
                st.json(schema)
            
            # Display summary if available
            if summary_file.exists():
                with open(summary_file, 'r', encoding='utf-8') as f:
                    summary_content = f.read()
                
                st.subheader("📝 Column Summary")
                st.text(summary_content)
            
            # Show CSV preview
            try:
                df = pd.read_csv(csv_file, nrows=5)  # Show first 5 rows
                st.subheader("📊 Data Preview (First 5 rows)")
                st.dataframe(df)
            except Exception as e:
                st.warning(f"Could not preview CSV: {str(e)}")

def main():
    # Header
    st.markdown('<div class="main-header">🔧 BigQuery Data Processor</div>', unsafe_allow_html=True)
    
    st.markdown("""
    This tool processes your Excel or CSV files for BigQuery compatibility and provides:
    - **Smart data type inference** (STRING, INT64, FLOAT64, BOOL, DATE, TIMESTAMP)
    - **Automatic column name sanitization** (BigQuery-compatible naming)
    - **Data cleaning and normalization** (handles missing values, formats dates)
    - **BigQuery schema generation** (ready for import)
    - **Clean CSV output** (properly formatted for BigQuery)
    
    **Smart Logic**: Columns containing any letters (A-Z) are automatically detected as STRING type.
    
    Simply upload your file and click "Process File" - no configuration needed!
    """)
    
    # File upload
    st.subheader("📁 Upload Your Data File")
    
    uploaded_file = st.file_uploader(
        "Choose an Excel (.xlsx, .xls) or CSV file",
        type=['xlsx', 'xls', 'csv'],
        help="Maximum file size: 200MB"
    )
    
    if uploaded_file is not None:
        # Check file size (200MB limit)
        file_size_mb = len(uploaded_file.getvalue()) / (1024 * 1024)
        
        if file_size_mb > 200:
            st.error(f"❌ File size ({file_size_mb:.1f}MB) exceeds the 200MB limit. Please upload a smaller file.")
            return
        
        st.success(f"✅ File uploaded successfully ({file_size_mb:.1f}MB)")
        
        # Process button
        if st.button("🚀 Process File", type="primary"):
            
            with st.spinner("Processing your file..."):
                # Create temporary directory for processing
                with tempfile.TemporaryDirectory() as temp_dir:
                    temp_dir = Path(temp_dir)
                    
                    # Save uploaded file
                    input_path = temp_dir / uploaded_file.name
                    with open(input_path, 'wb') as f:
                        f.write(uploaded_file.getvalue())
                    
                    # Create output directory
                    output_dir = temp_dir / "output"
                    output_dir.mkdir()
                    
                    # Process file based on extension
                    file_ext = input_path.suffix.lower()
                    
                    try:
                        if file_ext in {'.xlsx', '.xlsm', '.xls'}:
                            process_xlsx(input_path, output_dir)
                        elif file_ext == '.csv':
                            process_csv(input_path, output_dir)
                        else:
                            st.error("Unsupported file type. Please upload .xlsx, .xls, or .csv files.")
                            return
                        
                        # Display results
                        st.success("✅ Processing completed!")
                        display_processing_results(output_dir)
                        
                        # Create download package
                        zip_data = create_download_zip(output_dir)
                        
                        st.subheader("📦 Download Results")
                        st.download_button(
                            label="📥 Download All Results (ZIP)",
                            data=zip_data,
                            file_name=f"bigquery_processed_{uploaded_file.name.split('.')[0]}.zip",
                            mime="application/zip",
                            help="Download all processed files including CSV, schema, and summary files"
                        )
                        
                        # Individual file downloads
                        st.subheader("📄 Individual Files")
                        
                        # List all files in output directory
                        output_files = list(Path(output_dir).glob("*"))
                        for file_path in output_files:
                            if file_path.is_file():
                                with open(file_path, 'rb') as f:
                                    file_data = f.read()
                                
                                file_type = "📊 CSV" if file_path.suffix == '.csv' else "📋 JSON" if file_path.suffix == '.json' else "📝 TXT"
                                st.download_button(
                                    label=f"{file_type} Download {file_path.name}",
                                    data=file_data,
                                    file_name=file_path.name,
                                    mime="text/csv" if file_path.suffix == '.csv' else "application/json" if file_path.suffix == '.json' else "text/plain",
                                    key=f"file_{file_path.name}"
                                )
                    
                    except Exception as e:
                        st.error(f"❌ Processing failed: {str(e)}")
                        st.exception(e)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        <p>🔧 BigQuery Data Processor | Built with Streamlit</p>
        <p>Processes Excel/CSV files for BigQuery compatibility</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
