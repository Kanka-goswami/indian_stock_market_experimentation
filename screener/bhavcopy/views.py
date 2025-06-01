from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.http import JsonResponse
import requests
import pandas as pd
import numpy as np
import io
from datetime import datetime
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.http import JsonResponse
import requests
import pandas as pd
import io
from datetime import datetime
import time
from bhavcopy.models import Bhavcopy
import bhavcopy.constants as constant
import logging

logger = logging.getLogger(__name__)

class FetchBhavcopyDataView(APIView):
    """
    API view to fetch, process and store Bhavcopy data from external source
    """
    nse_main_url = "https://www.nseindia.com/"
    timeout= 4
    def get(self, request, *args, **kwargs):
        try:
            dt_str = request.GET.get("dt")
            if not dt_str:
                return Response({"error": "Missing 'dt' query parameter."}, status=status.HTTP_400_BAD_REQUEST)

            # Convert string to datetime object
            dt = datetime.strptime(dt_str, "%d-%m-%Y")
            # Format date components
            dd = dt.strftime('%d')
            mm = dt.strftime('%m')
            yyyy = dt.year
            
            # URL to fetch the CSV file
            csv_url = constant.link_bhavcopy.format(dd=dd, mm=mm, yyyy=yyyy)
            print(csv_url)
            
            # Setup headers for the request
            headers = {
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

            # Step 1: Hit the main NSE page first to get session cookies
            logger.info("Hitting NSE main page to establish session...")
            main_page_response = session.get(self.nse_main_url, timeout=self.timeout)
            
            if main_page_response.status_code != 200:
                logger.error(f"Failed to access NSE main page: {main_page_response.status_code}")
                return Response(
                    {"error": f"Failed to establish NSE session: {main_page_response.status_code}"},
                    status=status.HTTP_400_BAD_REQUEST
                )
                
            # Step 2: Wait a couple of seconds to avoid being flagged as a bot
            logger.info("Waiting before making bhavcopy request...")
            time.sleep(2)

            # Step 3: Use the same session (with cookies) to fetch the CSV file
            logger.info("Fetching bhavcopy data with established session...")
            response = session.get(csv_url, timeout=self.timeout)
            
            if response.status_code != 200:
                logger.error(f"Failed to fetch CSV: {response.status_code}")
                return Response(
                    {"error": f"Failed to fetch CSV: {response.status_code}"},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Use pandas to read the CSV content
            csv_content = io.StringIO(response.content.decode('utf-8'))
            df = pd.read_csv(csv_content)
            
            # Clean column names and handle potential spaces
            df.columns = df.columns.str.strip()
            
            # Convert date format (if necessary)
            df['DATE1'] = pd.to_datetime(df['DATE1'].str.strip(), format='%d-%b-%Y').dt.date
            
            df = self.clean_numerical_columns(df)
            # Process each row and save to database
            records_created = 0
            records_updated = 0
            
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
                    logger.error(f"Error processing row {data_dict['SYMBOL']}: {str(e)}")
            
            return Response({
                "message": "CSV processed successfully",
                "records_created": records_created,
                "records_updated": records_updated,
                "total_rows": len(df)
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Error processing CSV: {str(e)}")
            return Response(
                {"error": f"Error processing CSV: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    def clean_numerical_columns(self,df:None):
        """Handles the data cleaning for numerical columns."""
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
        return df