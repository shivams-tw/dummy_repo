import boto3
import datetime
import re
import subprocess
import sys

class ingestion_files:

    def __init__(self):
        self.data_sharing_zone_layer_bucket = 'test-edp-data-sharing-zone'
        self.bronze_layer_bucket="test-edp-bronze-layer"
        self.archive_folder="customer/cdm/archive/customer_identity/"
        self.data_sharing_bucket_ingestion_folder="customer/cdm/to_be_processed/customer_identity/"
        self.bronze_bucket_ingestion_folder = "customer/cdm/customer_identity/"
        self.AWS_PROFILE="default"  # Optional: Specify the AWS profile to use, if not default
        self.current_date = current_date = datetime.date.today().strftime("%Y%m%d")
        self.s3 = boto3.client('s3')

    def check_bucket_existence(self,bucket_name):

        try:
            self.s3.head_bucket(Bucket=bucket_name)
            return True
        except Exception as e:
            return False
            
    def check_folder_in_source_bucket(self,bucket_name, folder_prefix):

        try:
            # List objects in the bucket with the specified prefix
            response = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)

            # Check if any objects exist with the specified prefix
            if 'Contents' in response:
                return True
            else:
                return False
        except Exception as e:
            print(f'Failed checking the folder structure in {bucket_name} S3 bucket with error',e)
            sys.exit(1)
        
    def list_folders_in_s3_folder(self,bucket_name, folder_prefix):

        try:

            # List objects in the bucket with the specified prefix
            response = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix, Delimiter='/')

            # Extract folder names (prefixes) from response
            folders = []
            if 'CommonPrefixes' in response:
                for obj in response['CommonPrefixes']:
                    folders.append(obj['Prefix'].strip('/'))  # Remove trailing slash from folder name

            return folders
        
        except Exception as e:
            print(f'Failed fetching the folder structure in {bucket_name}{folder_prefix} S3 folder path with error',e)
            sys.exit(1)        

    def list_files_with_pattern(self,bucket_name, folder_prefix, file_pattern):

        try:

            # List objects in the bucket with the specified prefix
            response = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)

            # Check if any objects exist with the specified prefix
            files_matched = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    key = obj['Key'].split("/")[-1]
                    if not key.endswith('/'):  # Exclude folders themselves
                        if re.match(file_pattern, key):
                            files_matched.append(key)

            return files_matched
        
        except Exception as e:
            print(f'Failed listing files in {bucket_name}/{folder_prefix}/{file_pattern} S3 folder path with error',e)
            sys.exit(1)         
    
    def ingesting_files_dataSharingZone_to_bronze_layer(self):

        #Check if data sharing zone bucket and bronze bucket exists
        if self.check_bucket_existence(self.data_sharing_zone_layer_bucket):
            print(f"\nThe bucket '{self.data_sharing_zone_layer_bucket}' exists.")
        else:
            print(f"The bucket '{self.data_sharing_zone_layer_bucket}' does not exist.")
            sys.exit(1)

        if self.check_bucket_existence(obj.bronze_layer_bucket):
            print(f"The bucket '{self.bronze_layer_bucket}' exists. \n")
        else:
            print(f"The bucket '{self.bronze_layer_bucket}' does not exist. \n")
            sys.exit(1)


        #Check if some folder for is present to ingest the file s3://test-edp-data-sharing-zone/customer/cdm/to_be_processed/customer_identity/
        try:

            if self.check_folder_in_source_bucket(self.data_sharing_zone_layer_bucket,'customer/cdm/to_be_processed/customer_identity/'):
                print(f"The ingestion folder in Source bucket '{self.data_sharing_zone_layer_bucket}' exists. \n")
                
                # get the folder in which files would be files to ingestion
                folder_prefix = self.data_sharing_bucket_ingestion_folder

                folder_to_be_ingested = self.list_folders_in_s3_folder(self.data_sharing_zone_layer_bucket, folder_prefix)
                if folder_to_be_ingested:

                    for source_folder in folder_to_be_ingested:
                        print(f'{source_folder} to be ingested from Data Sharing Zone to Bronze bucket\n')

                        #get the files to be processed of specific pattern
                        file_pattern = r'^customer*'
                        folder_ingestion_date = datetime.datetime.strptime(source_folder.split('/')[-1], "%Y%m%d")
                        source_folder = source_folder + '/'

                        source_bucket_files_to_ingest = self.list_files_with_pattern(self.data_sharing_zone_layer_bucket, source_folder, file_pattern)
                        if source_bucket_files_to_ingest:
                            print('Source bucket files to be ingested',source_bucket_files_to_ingest)

                            #get the files available in bronze layer (Ideally there won't be any folder/file present because we have to copy the files into bronze layer)
                            target_folder = self.bronze_bucket_ingestion_folder + f'{folder_ingestion_date.year}/{str(folder_ingestion_date.month).zfill(2)}/{str(folder_ingestion_date.day).zfill(2)}/'
                            print('\nBronze Layer Bucket folder where files would be ingested',target_folder)
                            target_bucket_files_ingested = self.list_files_with_pattern(self.bronze_layer_bucket, target_folder, file_pattern)
                            if target_bucket_files_ingested:
                                print("Some Files are already ingested in bronze layer bucket folder")
                                print("Ingesting the files that are not present in the bronze layer bucket folder")
                                source_bucket_files_to_ingest_path_fix = [file_path.split('/')[-1] for file_path in source_bucket_files_to_ingest]
                                target_bucket_files_to_ingest_path_fix = [file_path.split('/')[-1] for file_path in target_bucket_files_ingested]

                                source_files_not_ingested = [item for item in source_bucket_files_to_ingest_path_fix if item not in target_bucket_files_to_ingest_path_fix]

                                if len(source_files_not_ingested) > 0:

                                    for files_to_be_ingested in source_files_not_ingested:

                                        command = f"aws s3 cp s3://{self.data_sharing_zone_layer_bucket}/{source_folder}{files_to_be_ingested} s3://{self.bronze_layer_bucket}/{target_folder}"

                                        # Execute the command using subprocess
                                        process = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                                        output, error = process.communicate()       

                                        if error:
                                            print("Error copying files:", error.decode())
                                            sys.exit(1)
                                        else:
                                            print(f"\n{files_to_be_ingested} File copied successfully! \n")         
                                
                                else:
                                    print(f'\nNo files Ingested as all the files from Data Sharing Zone to Bronze layer Bucket for {folder_ingestion_date.date()} are already ingested \n')
                            else:
                                print(f'\n No files found in Bronze layer bucket for {folder_ingestion_date.date()} ingested ')
                                print("Starting the ingestion of files from Data Sharing Zone to Bronze Layer bucket ")

                                #copy all the ingestion files from data sharing zone to bronze layer
                                # Construct the AWS CLI command
                                command = f"aws s3 cp s3://{self.data_sharing_zone_layer_bucket}/{source_folder} s3://{self.bronze_layer_bucket}/{target_folder} --recursive"

                                # Execute the command using subprocess
                                process = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                                output, error = process.communicate()

                                if error:
                                    print("Error copying files:", error.decode())
                                    sys.exit(1)
                                else:
                                    print("\n Files Ingestion from Data Sharing Zone to Bronze Layer is successfully! \n")
                        
                        else:
                            print('No Source bucket files found to be ingested')


                else:
                    print("No folders found in the specified folder.")
                    sys.exit(1)
            else:
                print(f"The ingestion folder in Source bucket '{self.data_sharing_zone_layer_bucket}' does not exist.")
                sys.exit(1)
        except Exception as e:
            print('Failed with error',e)
            sys.exit(1)

        

    def archiving_files_dataSharingZone_to_archive_folder(self):

       #Check if some folder for is present to archive
        try:

            if self.check_folder_in_source_bucket(self.data_sharing_zone_layer_bucket,'customer/cdm/to_be_processed/customer_identity/'):
                print(f"The ingestion folder in Source bucket '{self.data_sharing_zone_layer_bucket}' exists. \n")
                
                # get the folder in which files would be there to archive
                folder_prefix = self.data_sharing_bucket_ingestion_folder

                folder_to_be_ingested = self.list_folders_in_s3_folder(self.data_sharing_zone_layer_bucket, folder_prefix)
                if folder_to_be_ingested:

                    for source_folder in folder_to_be_ingested:
                        print(f'{source_folder} to be archived from Data Sharing Zone to Archive Foldert\n')

                        #get the files to be archived
                        file_pattern = r'^customer*'
                        folder_ingestion_date = datetime.datetime.strptime(source_folder.split('/')[-1], "%Y%m%d")
                        source_folder = source_folder + '/'

                        source_bucket_files_ingested = self.list_files_with_pattern(self.data_sharing_zone_layer_bucket, source_folder, file_pattern)
                        if source_bucket_files_ingested:
                            print('Source bucket files to be archived',source_bucket_files_ingested)
                            print('\n')

                            #get the files from bronze layer to check if the source files are already ingested so that they can be moved to archive folder
                            target_folder = self.bronze_bucket_ingestion_folder + f'{folder_ingestion_date.year}/{str(folder_ingestion_date.month).zfill(2)}/{str(folder_ingestion_date.day).zfill(2)}/'
                            print('\nBronze Layer Bucket folder where files are already ingested',target_folder)
                            target_bucket_files_ingested = self.list_files_with_pattern(self.bronze_layer_bucket, target_folder, file_pattern)
                            if target_bucket_files_ingested:

                                source_bucket_files_ingested_path_fix = [file_path.split('/')[-1] for file_path in source_bucket_files_ingested]
                                target_bucket_files_ingested_path_fix = [file_path.split('/')[-1] for file_path in target_bucket_files_ingested]

                                source_files_not_ingested = [item for item in source_bucket_files_ingested_path_fix if item not in target_bucket_files_ingested_path_fix]

                                if len(source_files_not_ingested) == 0:

                                    #copy all the ingestion files from data sharing zone to bronze layer
                                    command = f"aws s3 mv s3://{self.data_sharing_zone_layer_bucket}/{source_folder} s3://{self.data_sharing_zone_layer_bucket}/{self.archive_folder}{self.current_date}/{folder_ingestion_date.year}/{str(folder_ingestion_date.month).zfill(2)}/{str(folder_ingestion_date.day).zfill(2)}/ --recursive"
                                    process = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                                    output, error = process.communicate()

                                    if error:
                                        print("Error copying files:", error.decode())
                                        sys.exit(1)

                                    command = f"aws s3 rm s3://{self.data_sharing_zone_layer_bucket}/{source_folder} --recursive"
                                    process = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                                    output, error = process.communicate()

                                    if error:
                                        print("Error deleting empty folder files:", error.decode())
                                        sys.exit(1)
                                    else:
                                        print("\n Files archived successfully from Data Sharing Zone to Archive Folder \n")

                                else:
                                    print(f'{source_files_not_ingested} files are not ingested from Data Sharing Zone to Bronze Layer thus ingest them first')
                        else:
                            print('No Source bucket files found to be ingested')
        except Exception as e:
            print('Failed with error',e)


obj = ingestion_files()

obj.ingesting_files_dataSharingZone_to_bronze_layer()
obj.archiving_files_dataSharingZone_to_archive_folder()