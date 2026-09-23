
import shutil
import subprocess
import uuid
import zipfile
from pathlib import Path
from datetime import datetime, timezone

from common.config import Config

class ReleaseManager:

    def __init__(self, bucket_uri: str):
        self.bucket_uri = bucket_uri.strip('/')
        self.cfg = Config().dataproc_deploy


        # properties
        self.gcloud_ = None
        self.release_version_ = None
        self.release_uri_ = None

        self._resolve()

    @property
    def gcloud(self):
        if self.gcloud_ is None:
            self.gcloud_ = shutil.which("gcloud.cmd") or shutil.which("gcloud")
            if not self.gcloud_:
                raise RuntimeError("gcloud CLI is required")
        return self.gcloud_

    @property
    def release_version(self) -> str:
        if self.release_version_ is None:
            self.release_version_ = f"release-{datetime.now(timezone.utc):%Y%m%d_%H%M%S}-{uuid.uuid4().hex[:7]}"
        return self.release_version_

    @property
    def release_uri(self) -> str:
        if self.release_uri_ is None:
            self.release_uri_ = f"{self.bucket_uri}/{self.release_version.strip('/')}"
        return self.release_uri_

    def gcs_path_join(self, path: Path | str) -> str:
        return f"{self.release_uri}/{Path(path).as_posix().strip('/')}"

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
        path = Path(path).resolve()

        for dir_ in self.cfg.ignore_patterns.dirs:
            try:
                # If path is a subpath of dir_, this will succeed
                path.relative_to(dir_)
                return True
            except ValueError:
                pass

        if path.is_file():
            ext = path.suffix.lower()
            if ext in self.cfg.ignore_patterns.extensions:
                return True

        return False

    def zip_folder(self, folder: str | Path) -> str:
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

    def _create_new(self) -> str:
        """Create a release"""

        # include
        for p in self.cfg.assets.include:
            source = Path(p)

            if source.is_file():
                target = self.gcs_path_join(source)
                self.upload_file(source, target)

            elif source.is_dir():
                for src in source.rglob('*'):
                    if src.is_file() and not self.is_ignored(src):
                        target = self.gcs_path_join(src)
                        self.upload_file(src, target)

        # include_zipped
        for p in self.cfg.assets.include_zipped:
            source = self.zip_folder(p)
            target = self.gcs_path_join(source)
            self.upload_file(source, target)

    def _get_latest(self):
        """Get the latest release in the bucket"""
        res = subprocess.run(
            [self.gcloud, "storage", "ls", f"{self.bucket_uri}/"],
            capture_output=True,
            text=True,
        )
        if res.returncode != 0:
            details = res.stderr.strip() or res.stdout.strip()
            raise RuntimeError(f"Could not list releases in '{self.bucket_uri}'.\n{details}")

        versions = [
            line.strip().rstrip("/").rsplit("/", 1)[-1]
            for line in res.stdout.splitlines()
            ]
        if not versions:
            raise RuntimeError(f"No releases found with prefix '{self.prefix}' in {self.bucket_uri}")

        self.release_version_ = max(versions, key=lambda v: v.split("-")[1])

    def is_exists(self, uri: str) -> bool:
        res = subprocess.run(
            [self.gcloud, "storage", "ls", uri.strip("/")],
            capture_output=True,
            text=True,
        )
        return res.returncode == 0 and bool(res.stdout.strip())

    def _resolve(self) -> str:
        """Select and validate the concrete release used by this run."""
        if self.cfg.release.create_new:
            self._create_new()

        elif self.cfg.release.requested_version == "latest":
            self._get_latest()

        else:
            self.release_version_ = self.cfg.release.requested_version
            if not self.is_exists(self.release_uri):
                raise ValueError(f"Release does not exist: {self.release_uri}")
                 