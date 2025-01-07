# Selenium-Playwright_Data_Scraping

This repository contains a Python script for automating data extraction, processing, and insertion into a database. The script leverages Playwright for web automation and Selenium for handling automation tasks, along with data processing libraries like Pandas and database integration using SQLAlchemy.

---

## Key Features

1. **Web Automation**:
   - Automates login to a secure web portal.
   - Handles form filling, dynamic selection of reports, and file downloads based on date and other parameters.

2. **Data Processing**:
   - Cleanses and validates downloaded data files.
   - Standardizes column names and structures the data.
   - Handles edge cases like missing rows, extra columns, and blank data.

3. **Database Integration**:
   - Inserts cleaned data into a SQL database.
   - Uses SQLAlchemy for seamless database operations.

4. **File Management**:
   - Automatically creates a directory structure based on the current date (`YYYYMM/DD`).
   - Stores downloaded files in organized folders.

5. **Email Notifications**:
   - Sends email alerts for task completion or failure (e.g., login errors, database insertion issues).
   - Provides detailed logs for troubleshooting.

6. **Custom Logging**:
   - Logs all activities and errors to a log file for easy tracking.

---

## Prerequisites

- **Python 3.8 or higher**
- **Google Chrome or Microsoft Edge** (for Playwright)
- **ChromeDriver or EdgeDriver** (automatically managed by Playwright)
- A database server (e.g., Microsoft SQL Server)
- Required Python libraries (listed in `requirements.txt`)

---

## Installation and Setup

### 1. Clone the Repository
```bash
git clone https://github.com/DebasisNandiCode/Selenium-Playwright_Data_Scraping.git
