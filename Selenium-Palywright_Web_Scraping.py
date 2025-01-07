import os
import sys
import asyncio
import nest_asyncio
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import urllib
from playwright.async_api import async_playwright
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Secure sensitive data using environment variables
DB_SERVER = os.getenv('DB_SERVER', 'xxxx.xxxx.xxxx.xxxx') # SQL Server IP
DB_DATABASE = os.getenv('DB_DATABASE', 'xxxxxxxxxx') # SQL Database name
DB_USERNAME = os.getenv('DB_USERNAME', 'xxxxxxxxx') # SQL User name
DB_PASSWORD = os.getenv('DB_PASSWORD', 'xxxxxxxxxxx') # SQL Password
password_encoded = urllib.parse.quote(DB_PASSWORD)
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD', 'xxxxxxxxxxx') # Email Password
emailPassword_encoded = urllib.parse.quote(EMAIL_PASSWORD)

# Configure logging
log_file_path = os.path.join(os.path.expanduser("~"), "automation.log")
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Function to send email notifications
def send_email(subject, body):
    sender_email = "xxxxxxxxx@xxxxxxxxxxx.com"
    receiver_email = "xxxxxxxxxxxx@xxxxxxxxxxx.com"

    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = receiver_email
    message['Subject'] = subject
    message.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP('smtp.office365.com', 587) as server:
            server.starttls()
            server.login(sender_email, EMAIL_PASSWORD)
            server.sendmail(sender_email, receiver_email, message.as_string())
        logging.info("Email sent successfully")
    except Exception as e:
        logging.error(f"Failed to send email: {e}")

# Scrub and clean the data
def scrub_data(file_path, campaign):
    try:
        df = pd.read_csv(file_path, encoding='latin1')

        # Strip leading/trailing spaces or tabs from column names
        if df.empty:
            send_email(f"{campaign} - No Data ", f"No Data in {campaign} in todays file")
            print("DataFrame is empty. Skipping insertion.")
        else:
            # Proceed with cleaning and insertion
            df.columns = df.columns.str.strip()

        if df.iloc[0].isnull().all() or (df.iloc[0].astype(str).str.strip() == '').all():
            df = pd.read_csv(file_path, header=1)
            logging.info("First row was blank. Updated header.")
        if df.shape[1] > 23:
            df = df.iloc[:, :23]  # Trim to 23 columns
            
        # Define column renaming mappings. Suppose you have downloaded four files, and in one or two files, one or more column names are different. In such cases, this mapping is required.
        Camp1_rename_cols = {'xxxxxxx': 'xxxxxxx'} # Put your column name if renaming require

        Camp2_rename_cols = {'xxxxxxxx' : 'xxxxxxxx', 'xxxxxxxx' : 'xxxxxxxxx'} # Put your column name if renaming require

        Camp3_rename_cols = {'xxxxxxxx' : 'xxxxxxxx', 'xxxxxxxx' : 'xxxxxxxx'} # Put your column name if renaming require

        # Put your .csv file column name and database table column name
        common_rename_cols = {
            'xxxxxxxx': 'xxxxxxxx',
            'xxxxxxxx': 'xxxxxxxx',
            'xxxxxxxx': 'xxxxxxxx',
            'xxxxxxxx': 'xxxxxxxx',
            'xxxxxxxx': 'xxxxxxxx',
            'xxxxxxxx': 'xxxxxxxx',
            'xxxxxxxx': 'xxxxxxxx',
            'xxxxxxxx': 'xxxxxxxx',
            'xxxxxxxx': 'xxxxxxxx',
            'xxxxxxxx': 'xxxxxxxx',
            'xxxxxxxx': 'xxxxxxxx',
        }

        # Apply column renaming based on campaign
        rename_map = common_rename_cols.copy()  # Start with the common columns
        if campaign == "Camp1":
            rename_map.update(Camp1_rename_cols)
        elif campaign == "Camp2":
            rename_map.update(Camp2_rename_cols)
        elif campaign == "Camp3":
            rename_map.update(Camp3_rename_cols)

        # Only rename columns that exist in the DataFrame
        df.rename(columns={col: rename_map[col] for col in df.columns if col in rename_map}, inplace=True)
        return df
    
    except Exception as e:
        logging.error(f"Error scrubbing data: {e}")
        return None

# Insert data into the database
def insert_data_to_db(df, table_name, Campaing):
    try:
        engine = create_engine(f"mssql+pyodbc://{DB_USERNAME}:{password_encoded}@{DB_SERVER}/{DB_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server")
        
        df.to_sql(table_name, con=engine, if_exists='append', index=False)

        send_email(f"Success! - {Campaing} Data to SQL Processing Complete", f"{Campaing} - {df.shape[0]} data inserted successfully into the SQL database.")
        print(f"{Campaing} - {df.shape[0]} data inserted successfully into the SQL database.")
        logging.info(f"Data successfully inserted into {table_name}")
        
    except Exception as e:
        send_email(f"Error! - Inserting {Campaing} Data into SQL Database", f"Error: {str(e)}")
        logging.error(f"Error inserting data into {table_name}: {e}")

# Main automation logic using Playwright
async def main():
    locations = ['Delhi', 'Kolkata']
    campaigns = ['Camp1', 'Camp2', 'Camp3', 'Camp4']

    base_dir = r"C:\Users\xxxxxxxx\xxxxxxxxxx\xxxxxxxxxx\xxxxxxxxxxx\Raw_Data"

    todaysDate = datetime.now()
    year_month = todaysDate.strftime("%Y%m")
    previousDate = todaysDate - timedelta(days=1)
    day = previousDate.strftime("%d")
    # Check if "yyyymm" folder exists
    year_month_dir = os.path.join(base_dir, year_month)
    if not os.path.exists(year_month_dir):
        os.makedirs(year_month_dir)
        print(f"Created a folder : {year_month_dir}")
    else:
        print(f"Folder already exists : {year_month_dir}")

    # Check if "dd" folder exists within the "yyyymm" folder
    day_dir = os.path.join(year_month_dir, day)
    if not os.path.exists(day_dir):
        os.makedirs(day_dir)
        print(f"Created a folder : {day_dir}")
    else:
        print(f"Folder already exists : {day_dir}")

    # Configure download directory
    download_dir = os.path.join(base_dir, year_month, day_dir)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()
        await page.goto("https://xxxxxxxxxxxxx/xxxxxxxxxxxx")

        # Login
        try:
            await page.fill('//*[@id="sign_in_user_name"]', "xxxxxxxxxxx")
            await page.fill('//*[@id="password_field"]', "xxxxxxxxxxxxxx")
            await page.click('//*[@id="login_area"]')
            logging.info("Logged in successfully")
        except Exception as e:
            logging.error(f"Login failed: {e}")
            send_email("Automation Failed: Login Error", f"Error: {e}")
            await browser.close()
            return

        for location in locations:
            for campaign in campaigns:
                try:
                    await page.goto("https://xxxxxxxxxx/xxxxxxxxxxx/xxxxxxxxxxxx/xxxxxxxxxxxxx")
                    await asyncio.sleep(2)

                    today = datetime.now()
                    previous_date = today - timedelta(days=1)

                    if today.weekday() == 0:
                        saturday_date = today - timedelta(days=2)
                        sunday_date = today - timedelta(days=1)
                        st_dt = saturday_date.strftime('%m/%d/%Y')
                        end_dt = sunday_date.strftime('%m/%d/%Y')
                    else:
                        st_dt = previous_date.strftime('%m/%d/%Y')
                        end_dt = previous_date.strftime('%m/%d/%Y')
                    
                    await page.fill('#from_date', st_dt)
                    await page.fill('#to_date', end_dt)

                    # Close date picker by clicking outside
                    await page.click("body")

                    await page.select_option('#ffoffice_id', label=location)
                    await asyncio.sleep(2)
                    await page.select_option('select[name="campaign"]', label=campaign)
                    await asyncio.sleep(5)

                    await page.click('#show', force=True)
                    await asyncio.sleep(5)
                    
                    # Download file
                    async with page.expect_download() as download_info:
                        await page.click('//*[@id="main_page_content"]')
                    download = await download_info.value
                    downloaded_file_path = os.path.join(download_dir, location + '_' + download.suggested_filename)
                    await download.save_as(downloaded_file_path)
                    
                    df = scrub_data(downloaded_file_path, campaign)

                    if df is not None:
                        insert_data_to_db(df, "table_name", campaign) # Mention your database table name in 'table_name' section
   
                except Exception as e:
                    send_email(f"Error! - Inserting Data into SQL Database", f"Error: {str(e)}")
                    logging.error(f"Error processing {location} - {campaign}: {e}")

        await browser.close()
        logging.info("Automation completed")


if __name__ == "__main__":
    nest_asyncio.apply()
    asyncio.run(main())
