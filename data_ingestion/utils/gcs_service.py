from google.cloud import storage


class GCSService:

    def __init__(self, gcs_client: storage.Client, bucket_name: str, incoming_blob: str, archive_blob: str): 
        self.gcs_client    = gcs_client
        self.bucket_name   = bucket_name
        self.incoming_blob = incoming_blob
        self.archive_blob  = archive_blob

    def list_incoming_files(self) -> list[storage.Blob]:
        bucket = self.gcs_client.bucket(self.bucket_name)
        return [
            blob
            for blob in bucket.list_blobs(prefix=self.incoming_blob)
            if not blob.name.endswith("/")
        ]

    def get_gcs_uri(self, blob: storage.Blob) -> str:
        return f"gs://{blob.bucket.name}/{blob.name}"

    def delete(self, blob: storage.Blob) -> None:
        blob.delete()
    
    def archive_file(self, blob: storage.Blob) -> None:
        bucket = self.gcs_client.bucket(self.bucket_name)
        src_name = blob.name.rsplit("/", 1)[-1]
        src_name_prefix = src_name.split("_")[0]
        dest_name = f"{self.archive_blob}/{src_name_prefix}/{src_name}"
        bucket.copy_blob(blob, bucket, dest_name,)
        self.delete(blob)
