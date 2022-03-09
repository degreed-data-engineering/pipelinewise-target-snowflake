"""
Azure Blob Storage Upload Client
"""
import os
import datetime
 
from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContentSettings
 
from snowflake.connector.encryption_util import SnowflakeEncryptionUtil
from snowflake.connector.storage_client import SnowflakeFileEncryptionMaterial

from .base_upload_client import BaseUploadClient


class AzureBlobUploadClient(BaseUploadClient):
    """Azure Upload Client class"""

    def __init__(self, connection_config):
        super().__init__(connection_config)
        self.azure_client = self._create_azure_client()

    def _create_azure_client(self, config=None):
        if not config:
            config = self.connection_config
 
        az_account = config.get('azure_storage_account')
        az_key = config.get('azure_storage_key') 

        block_blob_service = BlockBlobService(
            account_name=az_account, 
            account_key=az_key)
        
        return block_blob_service
 

    def upload_file(self, file, stream, temp_dir=None):
        """Upload file to an external snowflake stage on azure blob storage"""
        # Generating key in blob storage
        az_account = self.connection_config['azure_storage_account']
        timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S-%f")

        blob = f"pipelinewise_{stream}_{timestamp}_{os.path.basename(file)}"
        self.logger.info('Target Azure Blob Storage: %s, local file: %s, blob: %s', az_account, file, blob)

        # create blob
        self.azure_client.create_blob_from_path(
            'target-snowflake',
            blob,
            file,
            content_settings=ContentSettings(content_type='application/CSV')
            )

        return blob

    
    def delete_object(self, stream: str, key: str) -> None:
        """Delete object from an external snowflake stage on Azure"""
        self.logger.info('Deleting %s from external snowflake stage on azure', key)
        self.azure_client.delete_blob('target-snowflake', key)
        #self.azure_client.delete_blob(file, content_settings=file_content_settings)

    def copy_object(self, copy_source: str, archive_container: str, prefixed_archive_container: str, archive_metadata: dict) -> None:
        """Copy object to another location on Azure Storage"""
        self.logger.info('Copying %s to %s/%s', copy_source, archive_container, prefixed_archive_container)
    
        source_container, source_key = copy_source.split("/", 1)
        az_account = self.connection_config['azure_storage_account']
        
        #TODO: confirm metadata is being captured.  
        # Might need to add:
        #   self.azure_client.get_blob_metadata(container_name=,blob_name=) 
        #   blob_service.set_blob_metadata(container_name="test",
        #                          blob_name="library_test.csv",
        #                          metadata={"New_test": "again_testing", "try": "this"})
        self.azure_client.copy_blob(container_name=prefixed_archive_container ,blob_name=source_key,copy_source='https://{}.blob.core.windows.net/{}'.format(az_account, copy_source))
        
        
        