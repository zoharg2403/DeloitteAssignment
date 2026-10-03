
import shutil
import subprocess
import zipfile
from pathlib import Path


class Packager:

    def __init__(self, release_uri:str, local_dir: Path | str, assets: dict, ignore_patterns: dict):
        self.release_uri     = release_uri
        self.local_dir       = Path(local_dir)
        self.assets          = assets
        self.ignore_patterns = ignore_patterns
        
        self.gcloud_ = None

    @property
    def gcloud(self):
        if self.gcloud_ is None:
            self.gcloud_ = shutil.which("gcloud.cmd") or shutil.which("gcloud")
            if not self.gcloud_:
                raise RuntimeError("gcloud CLI is required")
        return self.gcloud_
    
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

        for dir_ in self.ignore_patterns.dirs or []:
            try:
                # If path is a subpath of dir_, this will succeed
                path.relative_to(dir_)
                return True
            except ValueError:
                pass

        if path.is_file():
            ext = path.suffix.lower()
            if ext in self.ignore_patterns.extensions or []:
                return True

        return False

    def zip_as_python_package(self, folder: str | Path) -> str:
        """Zip a folder and return the path to the zip file. Include the source directory itself."""
        folder = Path(folder)
        if not folder.is_dir():
            raise NotADirectoryError(f"Folder does not exist: {folder}")

        zip_path = self.local_dir / folder.parent / f"{folder.name}.zip"
        zip_path.parent.mkdir(exist_ok=True, parents=True)
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for path in folder.rglob("*"):
                if path.is_file() and not self.is_ignored(path):
                    zip_file.write(path, path)

        return zip_path

    def zip_as_archive(self, folder: str | Path) -> str:
        """Zip a folder and return the path to the zip file. Don't include the source directory itself."""
        folder = Path(folder)
        if not folder.is_dir():
            raise NotADirectoryError(f"Folder does not exist: {folder}")

        zip_path = self.local_dir / folder.parent / f"{folder.name}.zip"
        zip_path.parent.mkdir(exist_ok=True, parents=True)
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for path in folder.rglob("*"):
                if path.is_file() and not self.is_ignored(path):
                    zip_file.write(path, Path(*path.parts[1:]))

        return zip_path

    def create(self):
        
        # files
        for p in self.assets.files or []:
            source = Path(p)
            if source.is_file():
                target = self.gcs_path_join(source)
                self.upload_file(source, target)

            elif source.is_dir():
                for src in source.rglob('*'):
                    if src.is_file() and not self.is_ignored(src):
                        target = self.gcs_path_join(src)
                        self.upload_file(src, target)

        # python_packages
        for p in self.assets.python_packages or []:
            source = self.zip_as_python_package(p)
            target = self.gcs_path_join(source.relative_to(self.local_dir))
            self.upload_file(source, target)

        # archive
        for p in self.assets.archive or []:
            source = self.zip_as_archive(p)
            target = self.gcs_path_join(source.relative_to(self.local_dir))
            self.upload_file(source, target)

