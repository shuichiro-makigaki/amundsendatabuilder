# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Iterator, Union

from pyhocon import ConfigFactory, ConfigTree

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.models.table_owner import TableOwner

LOGGER = logging.getLogger(__name__)


class SnowflakeTableOwnerExtractor(Extractor):
    """
    Extracts Snowflake table last update time from INFORMATION_SCHEMA metadata tables using SQLAlchemyExtractor.
    Requirements:
        snowflake-connector-python
        snowflake-sqlalchemy
    """
    # https://docs.snowflake.com/en/sql-reference/info-schema/views.html#columns
    # 'last_altered' column in 'TABLES` metadata view under 'INFORMATION_SCHEMA' contains last time when the table was
    # updated (both DML and DDL update). Below query fetches that column for each table.
    SQL_STATEMENT = """
        SELECT
            lower({cluster_source}) AS cluster,
            lower(t.table_schema) AS schema,
            lower(t.table_name) AS table_name,
            lower(t.table_owner) AS table_owner
        FROM
            {database}.INFORMATION_SCHEMA.TABLES t
        {where_clause_suffix};
        """

    # CONFIG KEYS
    WHERE_CLAUSE_SUFFIX_KEY = 'where_clause_suffix'
    CLUSTER_KEY = 'cluster_key'
    USE_CATALOG_AS_CLUSTER_NAME = 'use_catalog_as_cluster_name'
    # Database Key, used to identify the database type in the UI.
    DATABASE_KEY = 'database_key'
    # Snowflake Database Key, used to determine which Snowflake database to connect to.
    SNOWFLAKE_DATABASE_KEY = 'snowflake_database'
    ROLE_EMAIL_MAPPING = 'role_user_mapping'

    # Default values
    DEFAULT_CLUSTER_NAME = 'master'

    DEFAULT_CONFIG = ConfigFactory.from_dict(
        {
            WHERE_CLAUSE_SUFFIX_KEY: ' WHERE t.table_owner IS NOT NULL ',
            CLUSTER_KEY: DEFAULT_CLUSTER_NAME,
            USE_CATALOG_AS_CLUSTER_NAME: True,
            DATABASE_KEY: 'snowflake',
            SNOWFLAKE_DATABASE_KEY: 'prod',
            ROLE_EMAIL_MAPPING: lambda role: role,
        }
    )

    def init(self, conf: ConfigTree) -> None:
        conf = conf.with_fallback(SnowflakeTableOwnerExtractor.DEFAULT_CONFIG)
        self._cluster = conf.get_string(SnowflakeTableOwnerExtractor.CLUSTER_KEY)

        if conf.get_bool(SnowflakeTableOwnerExtractor.USE_CATALOG_AS_CLUSTER_NAME):
            cluster_source = "t.table_catalog"
        else:
            cluster_source = f"'{self._cluster}'"

        self._database = conf.get_string(SnowflakeTableOwnerExtractor.DATABASE_KEY)
        self._snowflake_database = conf.get_string(SnowflakeTableOwnerExtractor.SNOWFLAKE_DATABASE_KEY)

        self.sql_stmt = SnowflakeTableOwnerExtractor.SQL_STATEMENT.format(
            where_clause_suffix=conf.get_string(SnowflakeTableOwnerExtractor.WHERE_CLAUSE_SUFFIX_KEY),
            cluster_source=cluster_source,
            database=self._snowflake_database
        )

        LOGGER.info('SQL for snowflake table owner: %s', self.sql_stmt)

        # use an sql_alchemy_extractor to execute sql
        self._alchemy_extractor = SQLAlchemyExtractor()
        sql_alch_conf = Scoped.get_scoped_conf(
            conf, self._alchemy_extractor.get_scope()
        ).with_fallback(
            ConfigFactory.from_dict({
                SQLAlchemyExtractor.EXTRACT_SQL: self.sql_stmt
            })
        )

        self._alchemy_extractor.init(sql_alch_conf)
        self._extract_iter: Union[None, Iterator] = None

        self._role_email_mapping = conf.get(SnowflakeTableOwnerExtractor.ROLE_EMAIL_MAPPING)

    def close(self) -> None:
        self._alchemy_extractor.close()

    def extract(self) -> Union[TableOwner, None]:
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return 'extractor.snowflake_table_owner'

    def _get_extract_iter(self) -> Iterator[TableOwner]:
        """
        Provides iterator of result row from SQLAlchemy extractor
        """
        tbl_owner_row = self._alchemy_extractor.extract()
        while tbl_owner_row:
            yield TableOwner(
                db_name=self._database,
                table_name=tbl_owner_row['table_name'],
                owners=[
                    self._role_email_mapping(tbl_owner_row['table_owner'])
                ],
                schema=tbl_owner_row['schema'],
                cluster=tbl_owner_row['cluster']
            )
            tbl_owner_row = self._alchemy_extractor.extract()
