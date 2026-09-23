
from pyspark.sql import functions as F

from dataproc.utils.job_context import JobContext


def main():
    ctx = JobContext.create("users_per_city")
    try:
        ctx.logger.info("Starting users_per_city job, with tables: source='%s', target='%s')",
                        ctx.cfg.source.fullname, ctx.cfg.target.fullname)

        loc_cols = ["country", "region", "city"]
        users_per_city = (
            ctx.bigquery.read(ctx.cfg.source.dataset, ctx.cfg.source.table)
            .dropna(subset=loc_cols)
            .groupBy(*loc_cols)
            .agg(F.countDistinct("user_id").alias("user_count"))
        )

        ctx.bigquery.write(
            users_per_city,
            ctx.cfg.target.dataset,
            ctx.cfg.target.table,
            mode=ctx.cfg.write_mode,
        )
        ctx.logger.info("users_per_city job completed successfully")
    except Exception:
        ctx.logger.exception("users_per_city job failed")
        raise
    finally:
        ctx.close()


if __name__ == "__main__":
    main()
