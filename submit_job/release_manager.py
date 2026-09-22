import shutil
import subprocess
import uuid
from datetime import datetime, timezone


class ReleaseManager:

    def __init__(self, bucket_uri: str, create_new: bool, requested_version: str):
        self.bucket_uri = bucket_uri.strip('/')
        self.create_new = create_new
        self.requested_version = requested_version

        self.prefix = 'dev'

        self.gcloud_ = None

    @property
    def gcloud(self):
        if self.gcloud_ is None:
            self.gcloud_ = shutil.which("gcloud") or shutil.which("gcloud.cmd")
            if not self.gcloud_:
                raise RuntimeError("gcloud CLI is required")
        return self.gcloud_
        
    def _create_release_version(self) -> str:
        """Create a traceable release name"""
        return f"{self.prefix}-{datetime.now(timezone.utc):%Y%m%d_%H%M%S}-{uuid.uuid4().hex[:7]}"

    def _latest_release_version(self) -> str:
        """Return the latest release"""
        res = subprocess.run(
            [self.gcloud, "storage", "ls", f"{self.bucket_uri}/"],
            capture_output=True,
            text=True,
        )
        if res.returncode != 0:
            details = res.stderr.strip() or res.stdout.strip()
            raise RuntimeError(f"Could not list releases in '{self.bucket_uri}'.\n{details}")

        versions = []
        for line in res.stdout.splitlines():
            ver = line.strip().rstrip("/").rsplit("/", 1)[-1]
            if ver.startswith(self.prefix):
                versions.append(ver)
        if not versions:
            raise RuntimeError(f"No releases found with prefix '{self.prefix}' in {self.bucket_uri}")

        return max(versions, key=lambda v: v.split("-")[1])

    def release_exists(self, uri: str) -> bool:
        res = subprocess.run(
            [self.gcloud, "storage", "ls", uri[:-1]+"654/"],
            capture_output=True,
            text=True,
        )
        return res.returncode == 0 and bool(res.stdout.strip())

    def resolve_release(self):
        """Select and validate the concrete release used by this run."""
        if self.create_new:
            return self._create_release_version()

        requested = self.requested_version
        if not requested:
            raise ValueError(f"Rrequested release not specified")

        if requested == "latest":
            return self._latest_release_version()

        requested_uri = f"{self.bucket_uri}/{requested.strip('/')}/"
        if self.release_exists(requested_uri):
            return requested_uri.strip('/')

        raise ValueError(f"Release does not exist: {requested_uri}")
