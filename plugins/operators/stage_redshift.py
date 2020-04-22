from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries


class StageToRedshiftOperator(BaseOperator):
    """
    Executes a COPY command to load files from S3 to Redshift

    (I borrowed a lot of code from airflow.providers.amazon.aws.operators.S3ToRedshiftTransfer
     but since it had some limitations, I had to make a few changes)
    """

    template_fields = ()
    template_ext = ()
    ui_color = '#358140'

    @apply_defaults
    def __init__(
            self,
            schema,
            table_name,
            s3_url,
            redshift_conn_id,
            aws_conn_id,
            json_paths='auto',
            autocommit=True,
            *args, **kwargs,
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table_name = table_name
        self.s3_url = s3_url
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.json_paths = json_paths
        self.autocommit = autocommit

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id)
        credentials = self.s3.get_credentials()

        copy_query = SqlQueries.get_copy_json_query(
            schema=self.schema,
            table_name=self.table_name,
            s3_url=self.s3_url,
            aws_key=credentials.access_key,
            aws_secret=credentials.secret_key,
            json_paths=self.json_paths,
        )

        self.log.info('Executing COPY command...')
        self.hook.run(copy_query, self.autocommit)
        self.log.info("COPY command complete...")




