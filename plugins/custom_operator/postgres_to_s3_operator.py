import csv
import io
from typing import Sequence

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.context import Context

class PostgresToS3Operator(BaseOperator):
    """
    Executes a SQL query on postgres and uploads the result as CSV to S3.
    """

    template_fields: Sequence[str] = ("query", "s3_key")

    def __init__(
        self,
        *,
        postgres_conn_id: str,
        query: str,
        s3_conn_id: str,
        s3_bucket: str,
        s3_key: str,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.query = query
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context: Context):
        self.log.info("Running Postgres query")

        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)

        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(self.query)
                rows = cursor.fetchall()
                headers = [ col[0] for col in cursor.description]
        
        self.log.info("Fetched %d rows from Postgres", len(rows))

        data_buffer = io.StringIO()
        csv_writer = csv.writer(
            data_buffer,
            quoting=csv.QUOTE_MINIMAL, 
            lineterminator= "\n",
        )
        
        csv_writer.writerow(headers)
        csv_writer.writerows(rows)
        
        s3_hook.load_string(
            string_data = data_buffer.getvalue(),
            bucket_name = self.s3_bucket,
            key = self.s3_key,
            replace = True
        )

        self.log.info(
            "Uploaded %d rows to s3://%s/%s",
            len(rows),
            self.s3_bucket,
            self.s3_key
        )

