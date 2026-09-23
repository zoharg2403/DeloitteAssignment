
from pyspark.sql import functions as F

from dataproc.utils.job_context import JobContext


def main():
    ctx = JobContext.create("users_per_city")
    source_fullname = f"{ctx.cfg_job.source.dataset}.{ctx.cfg_job.source.table}"
    target_fullname = f"{ctx.cfg_job.target.dataset}.{ctx.cfg_job.target.table}"

    try:
        ctx.logger.info(f"Starting users_per_city job, with tables: source='{source_fullname}', target='{target_fullname}')")

        loc_cols = ["country", "region", "city"]
        users_per_city = (
            ctx.bigquery.read(ctx.cfg_job.source.dataset, ctx.cfg_job.source.table)
            .dropna(subset=loc_cols)
            .groupBy(*loc_cols)
            .agg(F.countDistinct("user_id").alias("user_count"))
        )

        ctx.bigquery.write(
            users_per_city,
            ctx.cfg_job.target.dataset,
            ctx.cfg_job.target.table,
            mode=ctx.cfg_job.write_mode,
        )
        ctx.logger.info("users_per_city job completed successfully")
    except Exception:
        ctx.logger.exception("users_per_city job failed")
        raise
    finally:
        ctx.close()


if __name__ == "__main__":
    main()
