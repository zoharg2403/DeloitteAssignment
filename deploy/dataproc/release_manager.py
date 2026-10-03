
import shutil
import subprocess
import uuid
import zipfile
from pathlib import Path
from datetime import datetime, timezone

from common.config import _Root
from deploy.dataproc.packager import Packager


class ReleaseManager:

    def __init__(self, bucket_uri: str, local_dir: str, create_new: bool, assets: _Root, ignore_patterns: _Root, requested_version: str | None = None):
        self.bucket_uri        = bucket_uri.strip('/')
        self.local_dir         = local_dir
        self.create_new        = create_new
        self.requested_version = requested_version
        self.assets            = assets
        self.ignore_patterns   = ignore_patterns

        # properties
        self.gcloud_          = None
        self.release_version_ = None
        self.release_uri_     = None

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

    def create_new_release(self) -> str:
        """Create a new release"""
        zp = Packager(
            release_uri     = self.release_uri,
            local_dir       = Path(self.local_dir) / self.release_version, 
            assets          = self.assets,
            ignore_patterns = self.ignore_patterns
        )
        zp.create()
    
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
            raise RuntimeError(f"No releases found in {self.bucket_uri}")

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
        if self.create_new:
            self.create_new_release()

        elif self.requested_version == "latest":
            self._get_latest()

        else:
            self.release_version_ = self.requested_version
            if not self.is_exists(self.release_uri):
                raise ValueError(f"Release does not exist: {self.release_uri}")
                 