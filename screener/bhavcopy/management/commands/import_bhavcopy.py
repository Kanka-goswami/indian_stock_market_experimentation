from django.core.management.base import BaseCommand
import requests
import datetime
import pandas as pd
import io
from datetime import datetime
from bhavcopy.models import Bhavcopy
import logging
import bhavcopy.constants as constant

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Fetch and process Bhavcopy data from external source'
    timeout = 4

    def add_arguments(self, parser):
        parser.add_argument('--date', type=str, help='Date in YYYY-MM-DD format', required=False)

    def get(self, dt):
        """
        Fetch Bhavcopy data from the external source.
        
        Args:
            dt: datetime object for the date to fetch
            
        Returns:
            pandas DataFrame with the fetched data or None if failed
        """
        try:
            self.stdout.write(self.style.SUCCESS('Fetching Bhavcopy data...'))
            
            # Format date components
            dd = dt.strftime('%d')
            mm = dt.strftime('%m')
            yyyy = dt.year
            
            # URL to fetch the CSV file
            csv_url = constant.link_bhavcopy.format(dd=dd, mm=mm, yyyy=yyyy)
            print(csv_url)
            
            # Setup headers for the request
            headers = {
                "Host": "www.niftyindices.com",
                "Referer": "https://www.nseindia.com",
                "X-Requested-With": "XMLHttpRequest",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36",
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            }
            
            # Create a session and update headers
            session = requests.Session()
            session.headers.update(headers)
            
            # Fetch the CSV file
            response = session.get(csv_url, timeout=self.timeout)
            
            if response.status_code != 200:
                self.stdout.write(self.style.ERROR(f"Failed to fetch CSV: {response.status_code}"))
                return None
            
            # Parse CSV content with pandas
            csv_content = io.StringIO(response.content.decode('utf-8'))
            df = pd.read_csv(csv_content)
            
            # Clean column names and handle potential spaces
            df.columns = df.columns.str.strip()
            
            # Convert date format
            df['DATE1'] = pd.to_datetime(df['DATE1'], format='%d-%b-%Y').dt.date
            
            return df
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error fetching data: {str(e)}"))
            return None

    def update_database(self, df):
        """
        Update the SQLite database with the fetched data using Django ORM.
        
        Args:
            df: pandas DataFrame with the data to update
            
        Returns:
            tuple: (records_created, records_updated)
        """
        if df is None or df.empty:
            self.stdout.write(self.style.WARNING("No data to update."))
            return 0, 0
            
        records_created = 0
        records_updated = 0
        
        try:
            for _, row in df.iterrows():
                # Convert row to dict
                data_dict = row.to_dict()
                
                # Try to get existing record or create new one
                try:
                    obj, created = Bhavcopy.objects.update_or_create(
                        SYMBOL=data_dict['SYMBOL'],
                        SERIES=data_dict['SERIES'],
                        DATE1=data_dict['DATE1'],
                        defaults={
                            'PREV_CLOSE': data_dict['PREV_CLOSE'],
                            'OPEN_PRICE': data_dict['OPEN_PRICE'],
                            'HIGH_PRICE': data_dict['HIGH_PRICE'],
                            'LOW_PRICE': data_dict['LOW_PRICE'],
                            'LAST_PRICE': data_dict['LAST_PRICE'],
                            'CLOSE_PRICE': data_dict['CLOSE_PRICE'],
                            'AVG_PRICE': data_dict['AVG_PRICE'],
                            'TTL_TRD_QNTY': data_dict['TTL_TRD_QNTY'],
                            'TURNOVER_LACS': data_dict['TURNOVER_LACS'],
                            'NO_OF_TRADES': data_dict['NO_OF_TRADES'],
                            'DELIV_QTY': data_dict['DELIV_QTY'],
                            'DELIV_PER': data_dict['DELIV_PER'],
                        }
                    )
                    
                    if created:
                        records_created += 1
                    else:
                        records_updated += 1
                        
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"Error processing row {data_dict['SYMBOL']}: {str(e)}"))
            
            return records_created, records_updated
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error updating database: {str(e)}"))
            return 0, 0

    def handle(self, *args, **options):
        """
        Main command handler that coordinates fetching and storing data.
        """
        try:
            # Get date from arguments or use current date
            date_str = options.get('date')
            if date_str:
                dt = datetime.strptime(date_str, '%Y-%m-%d')
            else:
                dt = datetime.now()
                
            self.stdout.write(self.style.SUCCESS(f'Starting Bhavcopy data fetch for {dt.strftime("%Y-%m-%d")}...'))
            
            # Fetch data
            df = self.get(dt)
            
            if df is not None:
                # Update database
                records_created, records_updated = self.update_database(df)
                
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Process completed. Created: {records_created}, Updated: {records_updated}, Total: {len(df)}"
                    )
                )
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error in command execution: {str(e)}"))

