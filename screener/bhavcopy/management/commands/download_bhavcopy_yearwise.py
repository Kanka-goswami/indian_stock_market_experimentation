# For command line execution as a management command
from django.core.management.base import BaseCommand
import time
from bhavcopy.yearly_bhavcopy_download_views import YearlyBhavcopyDownloaderView
import datetime

class Command(BaseCommand):
    help = 'Download Bhavcopy data for an entire year'
    
    def add_arguments(self, parser):
        parser.add_argument('year', type=int, help='Year to download data for')
        parser.add_argument('--start_from', type=str, help='Optional date to start from (DD-MM-YYYY)')
        
    def handle(self, *args, **options):
        year = options['year']
        start_from = options.get('start_from')
        
        downloader = YearlyBhavcopyDownloaderView()
        business_days = downloader._get_business_days(year)
        
        # Filter dates if start_from is provided
        if start_from:
            start_dt = datetime.strptime(start_from, "%d-%m-%Y")
            business_days = [d for d in business_days if d >= start_dt]
        
        self.stdout.write(self.style.SUCCESS(f"Starting download for year {year}. Total dates: {len(business_days)}"))
        
        # Track progress
        successful_dates = 0
        failed_dates = 0
        skipped_dates = 0
        
        # Establish initial session
        session = downloader._establish_session()
        if not session:
            self.stdout.write(self.style.ERROR("Could not establish initial session. Aborting."))
            return
        
        # Track when to refresh the session
        request_count = 0
        
        for dt in business_days:
            try:
                # Check if we need to refresh the session
                if request_count >= downloader.session_refresh_interval:
                    self.stdout.write("Refreshing session...")
                    session = downloader._establish_session()
                    if not session:
                        self.stdout.write(self.style.ERROR("Failed to refresh session. Retrying..."))
                        time.sleep(30)  # Wait longer before retry
                        session = downloader._establish_session()
                        if not session:
                            self.stdout.write(self.style.ERROR("Could not re-establish session after retry. Aborting."))
                            break
                    request_count = 0
                
                # Process the data for this date
                result = downloader._process_bhavcopy_data(session, dt)
                
                # Update counters
                if result["status"] == "success":
                    successful_dates += 1
                    self.stdout.write(self.style.SUCCESS(f"Success: {dt.strftime('%d-%m-%Y')} - Created: {result['records_created']}, Updated: {result['records_updated']}"))
                elif result["status"] == "failed":
                    failed_dates += 1
                    self.stdout.write(self.style.ERROR(f"Failed: {dt.strftime('%d-%m-%Y')} - {result.get('error', 'Unknown error')}"))
                else:  # skipped
                    skipped_dates += 1
                    self.stdout.write(self.style.WARNING(f"Skipped: {dt.strftime('%d-%m-%Y')} - {result.get('reason', 'Unknown reason')}"))
                
                # Log progress
                total_processed = successful_dates + failed_dates + skipped_dates
                self.stdout.write(f"Progress: {total_processed}/{len(business_days)} dates processed. " +
                               f"Success: {successful_dates}, Failed: {failed_dates}, Skipped: {skipped_dates}")
                
                # Increment request counter
                request_count += 1
                
                # Rate limiting - wait between requests
                time.sleep(3)  # 3 seconds between requests
                
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error processing date {dt.strftime('%d-%m-%Y')}: {str(e)}"))
                failed_dates += 1
        
        self.stdout.write(self.style.SUCCESS(f"Yearly download completed. Success: {successful_dates}, Failed: {failed_dates}, Skipped: {skipped_dates}"))