from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os

# Step 3: Set up Azure Blob Storage credentials
storage_account_name = "wecure"
storage_account_key = "IR/Hz2UGlQyW24oyqdHD6xrZ5CC22TLoz7ICRh4hiA4RzpJQ+I3T0UWXxgOJVnqV76GgyhudGh3u+AStdwUOzw=="
container_name = "raw"

# Step 4: Create a Blob Service Client
blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=storage_account_key)

# Step 5: Create a Container Client (create the container if it doesn't exist)
container_client = blob_service_client.get_container_client(container_name)

if not container_client.exists():
    container_client.create_container()

# Step 6: Upload CSV Files to the Container
local_csv_path = "C:\\Users\\abhimanyu.kutemate\\Desktop\\WeCure"

for root, dirs, files in os.walk(local_csv_path):
    for file_name in files:
        local_file_path = os.path.join(root, file_name)
        blob_client = container_client.get_blob_client(file_name)

        with open(local_file_path, "rb") as data:
            blob_client.upload_blob(data)
            print(f"Uploaded {file_name} to Azure Storage container.")

