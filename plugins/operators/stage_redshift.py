from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(
            self,
            redshift_conn_id='redshift_conn',
            iam_role_arn='',
            s3_data_path='',
            *args, **kwargs,
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.iam_role_arn = iam_role_arn
        self.s3_data_path = s3_data_path

    def execute(self, context):
        self.log.info('Connecting to Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        query = SqlQueries.get_copy_json_query(
            'song_staging',
            self.s3_data_path,
            self.iam_role_arn,
        )

        redshift.run('query')




