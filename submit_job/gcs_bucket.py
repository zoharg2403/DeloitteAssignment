import shutil
import subprocess
import zipfile
from pathlib import Path


class GCSBucketSync:
    """Package and upload the local assets required by a Dataproc job."""

    def __init__(self, bucket_uri: str, include: list[str], include_zipped: list[str], ignore_patterns: dict):
        self.bucket_uri = bucket_uri.rstrip("/")
        self.include = include
        self.include_zipped = include_zipped
        self.ignore_patterns = ignore_patterns

        self.gcloud_ = None

    @property
    def gcloud(self):
        if self.gcloud_ is None:
            self.gcloud_ = shutil.which("gcloud") or shutil.which("gcloud.cmd")
            if not self.gcloud_:
                raise RuntimeError("gcloud CLI is required to upload files to GCS")
        return self.gcloud_

    def upload_file(self, local: str | Path, remote: str | Path):
        """Upload a local file to GCS and return its GCS URI."""
        local, remote = str(local), str(remote)

        res = subprocess.run(
            [self.gcloud, "storage", "cp", local, remote], 
            capture_output=True, 
            text=True
            )
        if res.returncode != 0:
            details = res.stderr.strip() or res.stdout.strip()
            raise RuntimeError(f"GCS upload failed for {local} -> {remote}.\n{details}")

    def is_ignored(self, path: Path | str):
        path = Path(path)

        ignore_ext = self.ignore_patterns.get('extensions', None)
        if ignore_ext and path.suffix in ignore_ext:
            return True

        path_parts = set(path.parts)
        ignore_dirs = self.ignore_patterns.get('dirs', [])
        for p in ignore_dirs:
            if p in path_parts:
                return True

        return False

    def zip_asset(self, folder: str | Path) -> str:
        """Zip a folder and return the path to the zip file."""
        folder = Path(folder)
        if not folder.is_dir():
            raise NotADirectoryError(f"Folder does not exist: {folder}")

        zip_path = folder.parent / f"{folder.name}.zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for path in folder.rglob("*"):
                if path.is_file() and not self.is_ignored(path):
                    zip_file.write(path, path)
                
        return zip_path

    def create(self, release_version: str) -> dict:
        """Create release - Upload folders and files"""
        release_uri = f"{self.bucket_uri}/{release_version.strip("/")}"
        gcs_path_join = lambda p: f"{release_uri}/{Path(p).as_posix().strip('/')}"

        for path in self.include:
            source = Path(path)

            if source.is_file():
                target = gcs_path_join(source)
                self.upload_file(source, target)

            elif source.is_dir():
                for src in source.rglob('*'):
                    if src.is_file() and not self.is_ignored(src):
                        target = gcs_path_join(src)
                        self.upload_file(src, target)

        for path in self.include_zipped:
            source = self.zip_asset(path)
            target = gcs_path_join(source)
            self.upload_file(source, target)
