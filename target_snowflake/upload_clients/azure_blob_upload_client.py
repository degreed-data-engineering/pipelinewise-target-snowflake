"""
S3 Upload Client
"""
import os
import boto3
import datetime

import azure.storage.blob
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__


from azure.storage.blob import BlobClient 
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
 
        az_connection_string = config.get('azure_connection') 
        account_name =  config.get('azure_storage_account') 
        account_key =  config['azure_storage_key']
       #blob_service_client = BlobServiceClient.from_connection_string(container_name='target-snowflalke')
        blob_service_client = BlobServiceClient.from_connection_string(container_name='target-snowflalke') #instantiate new blobservice with connection string
        azure_client = blob_service_client.get_container_client(container='target-snowflake') #instantiate new containerclient
      
        
        return azure_client
 

    def upload_file(self, file, stream, temp_dir=None):
        """Upload file to an external snowflake stage on azure blob storage"""
        # Generating key in S3 bucket
        storage = self.connection_config['azure_storage_account']
        #s3_acl = self.connection_config.get('s3_acl') # DELETE???
        container_prefix = self.connection_config.get('azure_container_prefix', '')
        timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S-%f")

        blob_key = f"{container_prefix}pipelinewise_{stream}_{timestamp}_{os.path.basename(file)}"
        # target-snowflake/pipelinewise_UserOrganizationSettings_2022-03-05-01:13:11_filename
        self.logger.info('Target Azure Blob Storage: %s, local file: %s, S3 key: %s', storage, file, blob_key)

 
  
        self.azure_client.upload_blob(name=blob_key,  data=file, blob_type='csv/text')

        #upload_file(Filename, Bucket, Key, ExtraArgs=None, Callback=None, Config=None) S3
        #self.azure_client.upload_file(file, storage, blob_key, ExtraArgs=None)

        return blob_key

    
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
