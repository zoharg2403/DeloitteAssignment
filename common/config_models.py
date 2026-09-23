
from pydantic import BaseModel, Field, model_validator
from pathlib import Path


class ConfigLogger(BaseModel):
    name:      str  = Field("applog", min_length=1, description="Logger name")
    level:     str  = Field("INFO", min_length=1, description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
    format:    str  = Field("%(asctime)s - %(name)s - %(levelname)s - %(message)s", min_length=1, description="Logging format")
    logs_dir:  str  = Field("outputs/logs", description="dir path to save logs")
    to_stream: bool = Field(True, description="Toggle on (True) / off (False) logging.StreamHandler()")
    to_file:   bool = Field(True, description="Toggle on (True) / off (False) logging.FileHandler()")

    @model_validator(mode='after')
    def post_init(self):
        # logging level
        valid_lvl = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        self.level = self.level.upper()
        if self.level not in valid_lvl:
            raise ValueError(f"Invalid logging level '{self.level}'. Select from: {valid_lvl}")

        # format
        if not self.format.strip():
            raise ValueError("Logging format cannot be empty")

        # Must have at least one output
        if not self.to_stream and not self.to_file:
            raise ValueError("Invalid configuration: at least one of to_stream or to_file must be True.")

        # If to_file=True, logs_dir must be a non-empty string
        if self.to_file and not (self.logs_dir and self.logs_dir.strip()):
            raise ValueError("logs_dir must be a non-empty string when to_file=True")

        return self


# ----------------------------------------------------------------- #


class ConfigSparkSession(BaseModel):
    project_id:              str
    app_name:                str
    views_enabled:           bool
    materialization_dataset: str


class ConfigBigQuery(BaseModel):
    # location:    str
    temp_bucket: str


class ConfigEnv(BaseModel):
    env_name:       str
    project_id:     str
    region:         str
    scripts_bucket: str
    spark_session:  ConfigSparkSession
    bigquery:       ConfigBigQuery

    @model_validator(mode='before')
    def pre_init(self, values):
        # inject project_id into ConfigSparkSession and ConfigBigQuery
        proj_id = self["project_id"]
        for k in ["spark_session", "bigquery"]:
            val_dict = self.get(k, {})
            val_dict["project_id"] = proj_id
            self[k] = val_dict

        return self

    @model_validator(mode='after')
    def post_init(self):
        if not self.scripts_bucket.startswith('gs://'):
            raise ValueError("'scripts_bucket' must start with 'gs://'")
        self.scripts_bucket = self.scripts_bucket.strip("/")
        return self


# ----------------------------------------------------------------- #


class ConfigTable(BaseModel):
    dataset: str = Field(..., min_length=1)
    table: str   = Field(..., min_length=1)

    @property
    def fullname(self):
        return f"{self.dataset}.{self.table}"

class ConfigJob(BaseModel):
    batch_name: str     = Field(..., min_length=1)
    main_script: str    = Field(..., min_length=1)
    source: ConfigTable 
    target: ConfigTable
    write_mode: str = Field("overwrite", description="Output tables write mode")

    @model_validator(mode='after')
    def post_init(self):
        self.batch_name = self.batch_name.replace("_", "-")
        if not Path(self.main_script).is_file():
            raise FileNotFoundError(f"Main script '{self.main_script}' not found")
        return self


# ----------------------------------------------------------------- #


class ConfigAssets(BaseModel):
    include:         list[str] = Field(..., description="files to include")
    include_zipped:  list[str] = Field(..., description="folders to include as .zip file")   


class ConfigIgnorePatterns(BaseModel):
    dirs:       list[str | Path] = Field([], description="dirs / folder names to ignore")
    extensions: list[str | Path] = Field([], description="extensions patterns to ignore")

    @property
    def norm_dirs(self) -> list[Path]:
        return [Path(d).resolve() for d in self.dirs]

    @property
    def norm_extensions(self) -> list[Path]:
        norm_ext = set()
        for e in self.extensions:
            e = e.lower().lstrip("*").strip()
            if not e.startswith("."):
                e = "." + e
            norm_ext.add(e)
        return norm_ext


class ConfigRelease(BaseModel):
    create_new :       bool       = Field(False, description="If true, create new release version, else use 'requested_version'")
    requested_version: str | None = Field('latest', description="Specify version to use (or latest) when create_new=False")

    assets: ConfigAssets = Field(..., description="Define files to include in the release when create_new=True")
    ignore_patterns: ConfigIgnorePatterns = Field(..., description="Define dirs/extensions to exclude in the release when create_new=True")


# ----------------------------------------------------------------- #


class ConfigBatchKwargs(BaseModel):
    pyspark: dict[str, list[str]]
    runtime_config_properties: dict[str, bool | str]

    @model_validator(mode="after")
    def post_init(self):
        self.runtime_config_properties = {
            k: str(v).lower()
            for k, v in self.runtime_config_properties.items()
        }
        return self

    def map_bucket_fullpath(self, map_func):
        self.pyspark = {
            k: [map_func(p) for p in v] 
            for k, v in self.pyspark.items()
            if k in ['python_file_uris', 'file_uris']
            }


class ConfigJobSubmitter(BaseModel):
    env: ConfigEnv = Field(..., description="Enviroment Config")
    release: ConfigRelease = Field(..., description="kwargs for ReleaseManager")
    batch_kwargs: ConfigBatchKwargs = Field(..., description="batch kwargs to submit as part of the batch request")


# ----------------------------------------------------------------- #

    