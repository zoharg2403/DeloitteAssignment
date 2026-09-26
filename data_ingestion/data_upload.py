
from google.cloud.storage import Client
from google.api_core import exceptions as ge
from pathlib import Path
from datetime import datetime as dt

from common.config import Config
from common.logger import Logger


class GCSUpload:

    cfg = Config.load("data_ingestion/data_upload.yaml")
    logger = Logger()

    gcs_bucket_name = cfg.gcs.bucket_name.strip("/").lstrip("gs://")
    gcs_prefix = cfg.gcs.prefix.strip("/")

    @classmethod
    def upload_files(cls):
        cls.logger.info("Starting GCS Upload (source=%s, dest=%s)",
                        cls.cfg.local.root_dir, f"gs://{cls.gcs_bucket_name}/{cls.cfg.gcs.prefix}")

        client = Client()
        bucket = client.bucket(cls.gcs_bucket_name)

        root_dir = Path(cls.cfg.local.root_dir)
        files = {
            path 
            for pattern in cls.cfg.local.patterns
            for path in root_dir.rglob(pattern)
            if path.is_file()
        }
        nfiles = len(files)
        cls.logger.info(f"{nfiles} files found")

        failed = []
        for i, file in enumerate(files, start=1):
            try:
                blob = bucket.blob(f"{cls.gcs_prefix}/{file.name}")
                blob.upload_from_filename(file)

                # add metadata
                blob.metadata = {
                    "uploaded_from": "local_machine",
                    "upload_time": dt.now().isoformat()
                }
                blob.patch()

            except (ge.NotFound, ge.Unauthorized, ge.ServiceUnavailable) as e:
                cls.logger.error(f"File Upload Failed with error:\n{e}")
                raise e

            except Exception as e:
                failed.append((file, e))

            if i == 1 or i % 3 == 0 or i == nfiles:
                cls.logger.info(f"Upload file {i} / {nfiles}")

        if failed:
            cls.logger.info(f"File upload ended with {len(failed)} failed file(s)")
            for f, e in failed:
                cls.logger.warning(f"  - {file}: {e}")
        else:
            cls.logger.info("All files uploaded successfully!")


if __name__ == "__main__":
    GCSUpload.upload_files()
