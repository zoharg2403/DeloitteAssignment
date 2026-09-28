from google.cloud import storage


class GCSService:

    def __init__(self, gcs_client: storage.Client, bucket_name: str, incoming_blob: str, processed_blob: str, skipped_blob: str, failed_blob: str): 
        self.gcs_client     = gcs_client
        self.bucket_name    = bucket_name
        self.incoming_blob  = incoming_blob.strip("/")
        self.processed_blob = processed_blob.strip("/")
        self.skipped_blob   = skipped_blob.strip("/")
        self.failed_blob    = failed_blob.strip("/")

    def list_incoming_files(self) -> list[storage.Blob]:
        bucket = self.gcs_client.bucket(self.bucket_name)
        return [
            blob
            for blob in bucket.list_blobs(prefix=self.incoming_blob)
            if not blob.name.endswith("/")
        ]

    def _move_blob(self, src_blob: storage.Blob, dest_blob: str) -> None:
        bucket = self.gcs_client.bucket(self.bucket_name)
        src_name = src_blob.name.rsplit("/", 1)[-1]
        src_name_prefix = src_name.split("_")[0]
        dest_name = f"{dest_blob}/{src_name_prefix}/{src_name}"
        bucket.copy_blob(src_blob, bucket, dest_name)
        src_blob.delete()
    
    def archive_processed(self, blob: storage.Blob) -> None:
        self._move_blob(src_blob = blob, dest_blob = self.processed_blob)

    def archive_skipped(self, blob: storage.Blob) -> None:
        self._move_blob(src_blob = blob, dest_blob = self.skipped_blob)

    def archive_failed(self, blob: storage.Blob) -> None:
        self._move_blob(src_blob = blob, dest_blob = self.failed_blob)
