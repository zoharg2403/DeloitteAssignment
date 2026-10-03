
import shutil
import subprocess
from pathlib import Path

from common.config import Config


class DagSubmitter:

    def __init__(self):
        self.cfg = Config().load("config/airflow_pipelines/deploy.yaml")
        
        self.gcloud_ = None

    @property
    def gcloud(self):
        if self.gcloud_ is None:
            self.gcloud_ = shutil.which("gcloud.cmd") or shutil.which("gcloud")
            if not self.gcloud_:
                raise RuntimeError("gcloud CLI is required")
        return self.gcloud_

    @property
    def bucket_uri(self) -> str:
        return self.cfg.env.buckets.airflow_scripts

    def gcs_path_join(self, path: Path | str):
        return f"{self.bucket_uri}/dags/{Path(path).as_posix().strip('/')}"


    def is_ignored(self, path: Path | str):
        ignore_patterns = self.cfg.airflow_deploy.ignore_patterns
        path = Path(path).resolve()


        for dir_ in ignore_patterns.dirs:
            try:
                # If path is a subpath of dir_, this will succeed
                path.relative_to(dir_)
                return True
            except ValueError:
                pass

        if path.is_file():
            ext = path.suffix.lower()
            if ext in ignore_patterns.extensions:
                return True

        return False

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

    def upload_assets(self):
        assets = self.cfg.airflow_deploy.assets

        for p in assets.dags_files:
            source = Path(p)
            if source.is_file():
                target = self.gcs_path_join(source.name)
                self.upload_file(source, target)

            elif source.is_dir():
                for src in source.rglob('*'):
                    if src.is_file() and not self.is_ignored(src):
                        target = self.gcs_path_join(src.name)
                        self.upload_file(src, target)

        for p in assets.dags_subdirs:
            source = Path(p)
            if source.is_file():
                target = self.gcs_path_join(source)
                self.upload_file(source, target)

            elif source.is_dir():
                for src in source.rglob('*'):
                    if src.is_file() and not self.is_ignored(src):
                        target = self.gcs_path_join(src)
                        self.upload_file(src, target)

    def run(self):
        self.upload_assets()


if __name__ == "__main__":
    submitter = DagSubmitter()
    submitter.run()
