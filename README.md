# 🔍 BigQuery Data Validator

A lightweight Streamlit application that validates Excel and CSV files against BigQuery requirements, with automatic data type inference and schema generation.

## ✨ Features

- **📁 File Upload**: Support for Excel (.xlsx, .xls) and CSV files up to 200MB
- **🔍 Data Validation**: Comprehensive validation against BigQuery requirements
- **🤖 Smart Type Inference**: Automatic detection of data types (STRING, INT64, FLOAT64, BOOL, DATE, TIMESTAMP)
- **🧹 Column Sanitization**: Automatic conversion to BigQuery-compatible column names
- **📊 Quality Assessment**: Detailed validation reports with warnings and errors
- **🗂️ Schema Generation**: Ready-to-use BigQuery schema JSON files
- **📦 Batch Processing**: Handle multiple sheets in Excel files
- **📥 Download Results**: Get processed CSV files and schemas as downloadable packages

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- pip (Python package installer)

### Installation

1. **Clone or download this repository**
   ```bash
   git clone <repository-url>
   cd streamlit-bigquery-validator
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the application**
   ```bash
   streamlit run app.py
   ```

4. **Open your browser**
   - The app will automatically open at `http://localhost:8501`
   - If it doesn't open automatically, copy the URL from the terminal

## 📖 How to Use

### 1. Upload Your File
- Click "Choose an Excel (.xlsx, .xls) or CSV file" button
- Select your data file (maximum 200MB)
- Supported formats: `.xlsx`, `.xls`, `.csv`

### 2. Configure Settings (Optional)
- **Date Format**: Check if your dates are in DD/MM/YYYY format
- **Numeric Threshold**: Adjust sensitivity for numeric detection (default: 88%)
- **Date Threshold**: Adjust sensitivity for date detection (default: 65%)

### 3. Process Your Data
- Click "🚀 Process File" button
- Wait for processing to complete
- Review the validation results

### 4. Download Results
- **ZIP Package**: Download all files at once
- **Individual Files**: Download CSV and schema files separately

## 📊 What You Get

### Output Files
- **`filename.csv`**: Clean, BigQuery-ready CSV file
- **`filename_schema.json`**: BigQuery schema definition
- **`filename_summary.txt`**: Column type summary
- **`filename_validation.txt`**: Detailed validation report

### Validation Features
- ✅ **Column Name Validation**: Ensures BigQuery-compatible naming
- ✅ **Data Type Inference**: Smart detection of appropriate types
- ✅ **Date Format Detection**: Automatic date/timestamp recognition
- ✅ **Data Quality Checks**: Identifies invalid values and patterns
- ✅ **Quote Balance Validation**: Ensures proper CSV formatting

## 🔧 Advanced Configuration

### Supported Data Types
- **STRING**: Text data
- **INT64**: Integer numbers
- **FLOAT64**: Decimal numbers
- **BOOL**: Boolean values (true/false, yes/no, 1/0)
- **DATE**: Date values (YYYY-MM-DD)
- **TIMESTAMP**: Date and time values

### Column Name Rules
- Spaces are converted to underscores
- Special characters are removed or replaced
- Duplicate names get numeric suffixes
- Maximum length: 128 characters

### Date Detection
- Supports multiple date formats
- Excel serial date conversion
- Time component detection
- Configurable date format hints

## 🐛 Troubleshooting

### Common Issues

**File too large**
- Maximum file size is 200MB
- Consider splitting large files or using data compression

**Processing errors**
- Check file format (must be .xlsx, .xls, or .csv)
- Ensure file is not corrupted
- Try with a smaller sample file first

**Date format issues**
- Use the "Date format: DD/MM/YYYY" option if your dates are in European format
- Adjust the date detection threshold if needed

### Performance Tips
- For very large files, processing may take several minutes
- The app processes files in memory, so ensure sufficient RAM
- Consider using smaller sample files for testing

## 📝 Example Usage

1. **Upload a sales data Excel file**
2. **Configure date format** (if dates are DD/MM/YYYY)
3. **Process the file**
4. **Review validation results**:
   - Check for any warnings or errors
   - Verify detected data types
   - Review column name changes
5. **Download the results**:
   - Use the CSV file for BigQuery import
   - Use the schema JSON for table creation

## 🤝 Contributing

Feel free to submit issues, feature requests, or pull requests to improve this tool.

## 📄 License

This project is open source and available under the MIT License.

---

**Built with ❤️ using Streamlit**
