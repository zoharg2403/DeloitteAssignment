from pydantic import BaseModel, Field


class ConfigTable(BaseModel):
    dataset: str = Field(..., min_length=1)
    table:   str = Field(..., min_length=1)


class ConfigJob(BaseModel):
    main_script :   str = Field(..., description="Path to the main script for the job (should match both repo and GCS bucket paths)")
    source: ConfigTable
    target: ConfigTable
    write_mode:     str = "overwrite"
