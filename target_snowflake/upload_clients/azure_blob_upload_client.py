"""
S3 Upload Client
"""
import os
import boto3
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
 
        az_account =  config.get('azure_storage_account')
        az_key =  config.get('azure_storage_key') 

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
        bucket = self.connection_config['azure_storage_account']
        self.s3_client.delete_object(Bucket=bucket, Key=key)



        storage = self.connection_config['azure_storage_account']
        #self.azure_client.delete_blob(file, content_settings=file_content_settings)

    def copy_object(self, copy_source: str, target_bucket: str, target_key: str, target_metadata: dict) -> None:
        """Copy object to another location on S3"""
        self.logger.info('Copying %s to %s/%s', copy_source, target_bucket, target_key)
        source_bucket, source_key = copy_source.split("/", 1)
        metadata = self.s3_client.head_object(Bucket=source_bucket, Key=source_key).get('Metadata', {})
        metadata.update(target_metadata)
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.copy_object
        self.s3_client.copy_object(CopySource=copy_source, Bucket=target_bucket, Key=target_key,
                                   Metadata=metadata, MetadataDirective="REPLACE")
