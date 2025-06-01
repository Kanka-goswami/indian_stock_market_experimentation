from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.http import JsonResponse
import requests
import pandas as pd
import io
from datetime import datetime, timedelta
import time
import numpy as np
from bhavcopy.models import Bhavcopy
import bhavcopy.constants as constant
import logging

logger = logging.getLogger(__name__)

class YearlyBhavcopyDownloaderView(APIView):
    """
    API view to fetch, process and store Bhavcopy data for an entire year.
    Implements proper session management, rate limiting, and error handling.
    """
    timeout = 15
    nse_main_url = "https://www.nseindia.com/"
    session_refresh_interval = 50  # Refresh session after every 50 requests
    
    def _get_business_days(self, year):
        """Generate business days for the given year, excluding weekends."""
        start_date = f"01-01-{year}"
        end_date = f"31-12-{year}"
        
        # Convert string dates to datetime objects
        start_dt = datetime.strptime(start_date, "%d-%m-%Y")
        end_dt = datetime.strptime(end_date, "%d-%m-%Y")
        
        # Generate all business days (excluding weekends)
        date_range = []
        current_dt = start_dt
        
        while current_dt <= end_dt:
            # Weekday 0-4 corresponds to Monday-Friday
            if current_dt.weekday() < 5:
                date_range.append(current_dt)
            current_dt += timedelta(days=1)
            
        return date_range
    
    def _establish_session(self):
        """Establish a new session with NSE by hitting the main page."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
        
        session = requests.Session()
        session.headers.update(headers)
        
        logger.info("Establishing new session with NSE...")
        try:
            main_page_response = session.get(self.nse_main_url, timeout=self.timeout)
            
            if main_page_response.status_code != 200:
                logger.error(f"Failed to access NSE main page: {main_page_response.status_code}")
                return None
                
            logger.info("Session established successfully.")
            return session
        except Exception as e:
            logger.error(f"Error establishing session: {str(e)}")
            return None
    
    def _process_bhavcopy_data(self, session, dt):
        """Process Bhavcopy data for a specific date."""
        try:
            # Format date components
            dd = dt.strftime('%d')
            mm = dt.strftime('%m')
            yyyy = dt.year
            
            # URL to fetch the CSV file
            csv_url = constant.link_bhavcopy.format(dd=dd, mm=mm, yyyy=yyyy)
            date_str = dt.strftime('%d-%m-%Y')
            logger.info(f"Fetching bhavcopy for date: {date_str}")
            
            # Use the session to fetch the CSV file
            response = session.get(csv_url, timeout=self.timeout)
            
            if response.status_code != 200:
                logger.error(f"Failed to fetch CSV for {date_str}: {response.status_code}")
                return {
                    "date": date_str,
                    "status": "failed",
                    "error": f"HTTP {response.status_code}"
                }
            
            # Use pandas to read the CSV content
            csv_content = io.StringIO(response.content.decode('utf-8'))
            df = pd.read_csv(csv_content)
            
            if df.empty:
                logger.warning(f"Empty CSV data for {date_str}")
                return {
                    "date": date_str,
                    "status": "skipped",
                    "reason": "Empty data"
                }
            
            # Clean column names and handle potential spaces
            df.columns = df.columns.str.strip()
            
            # Convert date format (if necessary)
            df['DATE1'] = pd.to_datetime(df['DATE1'].str.strip(), format='%d-%b-%Y').dt.date
            
            # Clean numerical columns that might contain '-' or other non-numeric values
            numerical_columns = ['PREV_CLOSE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 
                               'LAST_PRICE', 'CLOSE_PRICE', 'AVG_PRICE', 'TTL_TRD_QNTY',
                               'TURNOVER_LACS', 'NO_OF_TRADES', 'DELIV_QTY', 'DELIV_PER']
            
            # Replace '-', ' -', '- ' or any non-numeric value with NaN and then with 0
            for col in numerical_columns:
                if col in df.columns:
                    # Convert to string first to handle any type of data
                    df[col] = df[col].astype(str).str.strip()
                    # Replace any variant of dash with empty string
                    df[col] = df[col].replace(['-', ' -', '- ', ' - '], '')
                    # Convert empty strings to NaN
                    df[col] = df[col].replace('', np.nan)
                    # Convert to float and replace NaN with 0
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # Process each row and save to database
            records_created = 0
            records_updated = 0
            records_error = 0
            
            # Use transaction for better performance and data integrity
            from django.db import transaction
            
            with transaction.atomic():
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
                        records_error += 1
                        logger.error(f"Error processing row {data_dict['SYMBOL']} for date {date_str}: {str(e)}")
            
            return {
                "date": date_str,
                "status": "success",
                "records_created": records_created,
                "records_updated": records_updated,
                "records_with_errors": records_error,
                "total_rows": len(df)
            }
            
        except Exception as e:
            logger.error(f"Error processing CSV for date {dt.strftime('%d-%m-%Y')}: {str(e)}")
            return {
                "date": dt.strftime('%d-%m-%Y'),
                "status": "failed",
                "error": str(e)
            }
    
    def get(self, request, *args, **kwargs):
        """API endpoint to trigger yearly bhavcopy download."""
        try:
            year = request.GET.get("year")
            if not year:
                return Response({"error": "Missing 'year' query parameter."}, status=status.HTTP_400_BAD_REQUEST)
                
            year = int(year)
            start_from = request.GET.get("start_from")  # Optional parameter to start from a specific date
            
            # Generate business days for the year
            business_days = self._get_business_days(year)
            
            # Filter dates if start_from is provided
            if start_from:
                start_dt = datetime.strptime(start_from, "%d-%m-%Y")
                business_days = [d for d in business_days if d >= start_dt]
            
            # Results tracking
            results = []
            successful_dates = 0
            failed_dates = 0
            skipped_dates = 0
            
            # For immediate response
            from threading import Thread
            
            def process_dates_in_background():
                nonlocal successful_dates, failed_dates, skipped_dates
                
                # Track when to refresh the session
                request_count = 0
                session = self._establish_session()
                
                if not session:
                    logger.error("Could not establish initial session. Aborting.")
                    return
                
                for dt in business_days:
                    try:
                        # Check if we need to refresh the session
                        if request_count >= self.session_refresh_interval:
                            logger.info("Refreshing session...")
                            session = self._establish_session()
                            if not session:
                                logger.error("Failed to refresh session. Retrying...")
                                time.sleep(30)  # Wait longer before retry
                                session = self._establish_session()
                                if not session:
                                    logger.error("Could not re-establish session after retry. Aborting.")
                                    break
                            request_count = 0
                        
                        # Process the data for this date
                        result = self._process_bhavcopy_data(session, dt)
                        results.append(result)
                        
                        # Update counters
                        if result["status"] == "success":
                            successful_dates += 1
                        elif result["status"] == "failed":
                            failed_dates += 1
                        else:  # skipped
                            skipped_dates += 1
                        
                        # Log progress
                        total_processed = successful_dates + failed_dates + skipped_dates
                        logger.info(f"Progress: {total_processed}/{len(business_days)} dates processed. " +
                                   f"Success: {successful_dates}, Failed: {failed_dates}, Skipped: {skipped_dates}")
                        
                        # Increment request counter
                        request_count += 1
                        
                        # Rate limiting - wait between requests
                        time.sleep(3)  # 3 seconds between requests
                        
                    except Exception as e:
                        logger.error(f"Error processing date {dt.strftime('%d-%m-%Y')}: {str(e)}")
                        failed_dates += 1
                
                logger.info(f"Yearly download completed. Success: {successful_dates}, Failed: {failed_dates}, Skipped: {skipped_dates}")
            
            # Start background processing
            thread = Thread(target=process_dates_in_background)
            thread.daemon = True
            thread.start()
            
            return Response({
                "message": f"Download started for year {year}. Processing {len(business_days)} business days in the background.",
                "total_dates": len(business_days),
                "status": "processing"
            }, status=status.HTTP_202_ACCEPTED)
            
        except Exception as e:
            logger.error(f"Error starting yearly download: {str(e)}")
            return Response(
                {"error": f"Error starting yearly download: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


