import streamlit as st
import pandas as pd
import json
import tempfile
import zipfile
from pathlib import Path
import io
import sys
import os

# Import the validation functions from main.py
from main import (
    process_xlsx, process_csv, RunLogger, 
    validate_headers, strip_cell, infer_column,
    bq_schema_from_df, format_dates_for_csv,
    write_clean_csv, find_unbalanced_quote_lines
)

# Page configuration
st.set_page_config(
    page_title="BigQuery Data Validator",
    page_icon="🔍",
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

def create_download_zip(results, temp_dir):
    """Create a zip file with all output files"""
    zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        # Add all files from temp directory
        for file_path in Path(temp_dir).rglob('*'):
            if file_path.is_file():
                zip_file.write(file_path, file_path.relative_to(temp_dir))
    
    zip_buffer.seek(0)
    return zip_buffer.getvalue()

def display_validation_results(results, temp_dir):
    """Display validation results in a user-friendly format"""
    
    if not results:
        st.error("No results to display")
        return
    
    # Overall summary
    total_errors = sum(r.get("errors", 0) for r in results)
    total_warnings = sum(r.get("warnings", 0) for r in results)
    total_sheets = len(results)
    clean_sheets = sum(1 for r in results if r.get("is_clean", False))
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Sheets", total_sheets)
    
    with col2:
        st.metric("Clean Sheets", clean_sheets, delta=f"{clean_sheets/total_sheets*100:.1f}%" if total_sheets > 0 else "0%")
    
    with col3:
        st.metric("Warnings", total_warnings, delta="⚠️" if total_warnings > 0 else "✅")
    
    with col4:
        st.metric("Errors", total_errors, delta="❌" if total_errors > 0 else "✅")
    
    # Status message
    if total_errors == 0 and total_warnings == 0:
        st.success("🎉 All validations passed! Your data is BigQuery-ready.")
    elif total_errors == 0:
        st.warning(f"⚠️ Validation completed with {total_warnings} warnings. Check details below.")
    else:
        st.error(f"❌ Validation completed with {total_errors} errors and {total_warnings} warnings.")
    
    # Detailed results for each sheet
    st.subheader("📊 Detailed Results")
    
    for i, result in enumerate(results):
        with st.expander(f"Sheet: {result.get('sheet', f'Sheet {i+1}')}", expanded=total_sheets == 1):
            
            # Sheet metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Rows", result.get('rows', 0))
            with col2:
                st.metric("Columns", result.get('cols', 0))
            with col3:
                status = "✅ Clean" if result.get('is_clean', False) else "⚠️ Issues"
                st.metric("Status", status)
            
            # Display validation report if available
            validation_path = result.get('validation_path')
            if validation_path and Path(validation_path).exists():
                with open(validation_path, 'r', encoding='utf-8') as f:
                    validation_content = f.read()
                
                st.subheader("📋 Validation Report")
                st.text(validation_content)
            
            # Display schema if available
            schema_path = result.get('schema_path')
            if schema_path and Path(schema_path).exists():
                with open(schema_path, 'r', encoding='utf-8') as f:
                    schema = json.load(f)
                
                st.subheader("🗂️ BigQuery Schema")
                st.json(schema)
            
            # Display summary if available
            summary_path = result.get('summary_path')
            if summary_path and Path(summary_path).exists():
                with open(summary_path, 'r', encoding='utf-8') as f:
                    summary_content = f.read()
                
                st.subheader("📝 Column Summary")
                st.text(summary_content)

def main():
    # Header
    st.markdown('<div class="main-header">🔍 BigQuery Data Validator</div>', unsafe_allow_html=True)
    
    st.markdown("""
    This tool validates your Excel or CSV files against BigQuery requirements and provides:
    - **Smart data type inference** (STRING, INT64, FLOAT64, BOOL, DATE, TIMESTAMP)
    - **Automatic column name sanitization** (BigQuery-compatible naming)
    - **Comprehensive data quality assessment** (validation warnings and errors)
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
                    
                    # Create logger
                    logger = RunLogger(output_dir)
                    
                    # Process file based on extension
                    file_ext = input_path.suffix.lower()
                    
                    try:
                        if file_ext in {'.xlsx', '.xlsm', '.xls'}:
                            results = process_xlsx(input_path, output_dir, logger)
                        elif file_ext == '.csv':
                            results = process_csv(input_path, output_dir, logger)
                        else:
                            st.error("Unsupported file type. Please upload .xlsx, .xls, or .csv files.")
                            return
                        
                        # Display results
                        st.success("✅ Processing completed!")
                        display_validation_results(results, output_dir)
                        
                        # Create download package
                        if results:
                            zip_data = create_download_zip(results, output_dir)
                            
                            st.subheader("📦 Download Results")
                            st.download_button(
                                label="📥 Download All Results (ZIP)",
                                data=zip_data,
                                file_name=f"bigquery_validation_{uploaded_file.name.split('.')[0]}.zip",
                                mime="application/zip",
                                help="Download all processed files including CSV, schema, and validation reports"
                            )
                            
                            # Individual file downloads
                            st.subheader("📄 Individual Files")
                            
                            for i, result in enumerate(results):
                                sheet_name = result.get('sheet', f'Sheet_{i+1}')
                                
                                # CSV download
                                csv_path = result.get('csv_path')
                                if csv_path and Path(csv_path).exists():
                                    with open(csv_path, 'rb') as f:
                                        csv_data = f.read()
                                    
                                    st.download_button(
                                        label=f"📊 Download {sheet_name} CSV",
                                        data=csv_data,
                                        file_name=f"{sheet_name}.csv",
                                        mime="text/csv",
                                        key=f"csv_{i}"
                                    )
                    
                    except Exception as e:
                        st.error(f"❌ Processing failed: {str(e)}")
                        st.exception(e)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        <p>🔍 BigQuery Data Validator | Built with Streamlit</p>
        <p>Validates Excel/CSV files for BigQuery compatibility</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
