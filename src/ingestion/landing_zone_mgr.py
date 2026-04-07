# src/ingestion/landing_zone_mgr.py
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient

class LandingZoneManager:
    """
    Manages the lifecycle of files in the ADLS Gen2 Landing Zone.
    Handles archiving processed files to prevent 'Listing Lag'.
    """

    def __init__(self, connection_string: str, container_name: str = "landing"):
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_client = self.blob_service_client.get_container_client(container_name)

    def archive_processed_files(self, source_prefix: str, archive_prefix: str = "archive"):
        """
        Moves files from 'raw/' to 'archive/YYYY/MM/DD/' after successful ingestion.
        """
        date_path = datetime.now().strftime("%Y/%m/%d")
        blobs = self.container_client.list_blobs(name_starts_with=source_prefix)

        for blob in blobs:
            # Construct the new archival path
            file_name = os.path.basename(blob.name)
            new_path = f"{archive_prefix}/{date_path}/{file_name}"
            
            # 1. Start Copy
            source_blob = f"{self.container_client.url}/{blob.name}"
            dest_blob = self.blob_service_client.get_blob_client(
                self.container_client.container_name, new_path
            )
            dest_blob.start_copy_from_url(source_blob)
            
            # 2. Delete Original
            self.container_client.delete_blob(blob.name)
            print(f"📦 Archived: {blob.name} -> {new_path}")

    def monitor_arrival_latency(self):
        """
        Lead-Level Logic: Checks for 'Stale' files in landing that 
        haven't been picked up by Auto Loader within the SLA window.
        """
        # Logic to alert if files > 2 hours old exist in landing
        pass