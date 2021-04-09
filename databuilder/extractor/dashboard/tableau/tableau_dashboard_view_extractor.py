# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import (
    Any, Dict, Iterator,
)

from pyhocon import ConfigFactory, ConfigTree

import databuilder.extractor.dashboard.tableau.tableau_dashboard_constants as const
from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.dashboard.tableau.tableau_dashboard_utils import (
    TableauDashboardUtils, TableauGraphQLApiExtractor,
)
from databuilder.extractor.restapi.rest_api_extractor import STATIC_RECORD_DICT
from databuilder.transformer.base_transformer import ChainedTransformer
from databuilder.transformer.dict_to_model import MODEL_CLASS, DictToModel

LOGGER = logging.getLogger(__name__)


class TableauGraphQLApiViewExtractor(TableauGraphQLApiExtractor):
    """
    Implements the extraction-time logic for parsing the GraphQL result and transforming into a dict
    that fills the DashboardQuery model. Allows workbooks to be exlcuded based on their project.
    """

    CLUSTER = const.CLUSTER
    EXCLUDED_PROJECTS = const.EXCLUDED_PROJECTS
    SITE_NAME = const.SITE_NAME
    TABLEAU_BASE_URL = const.TABLEAU_BASE_URL

    def execute(self) -> Iterator[Dict[str, Any]]:
        response = self.execute_query()
        base_url = self._conf.get(TableauGraphQLApiViewExtractor.TABLEAU_BASE_URL)
        site_name = self._conf.get_string(TableauGraphQLApiViewExtractor.SITE_NAME, '')
        site_url_path = ''
        if site_name != '':
            site_url_path = f'/site/{site_name}'
        for workbook in response['workbooks']:
            if None in (workbook['projectName'], workbook['name']):
                continue
            if workbook['projectName'] in self._conf.get_list(TableauGraphQLApiViewExtractor.EXCLUDED_PROJECTS, []):
                continue

            for view in workbook['views']:
                if view['path'] == '' or view['path'] is None:
                    continue
                data = {
                    'dashboard_group_id': workbook['projectName'],
                    'dashboard_id': TableauDashboardUtils.sanitize_workbook_name(workbook['name']),
                    'query_name': f'{view["index"]}. {view["name"]}',
                    'query_id': view['id'],
                    'query_text': f'{base_url}/#{site_url_path}/views/{view["path"]}',
                    'url': f'{base_url}/#{site_url_path}/views/{view["path"]}',
                    'cluster': self._conf.get_string(TableauGraphQLApiViewExtractor.CLUSTER)
                }
                yield data


class TableauDashboardViewExtractor(Extractor):
    """
    Extracts metadata about the queries associated with Tableau workbooks.
    In terms of Tableau's Metadata API, these queries are called "custom SQL tables".
    However, not every workbook uses custom SQL queries, and most are built with a mixture of using the
    datasource fields directly and various "calculated" columns.
    This extractor iterates through one query at a time, yielding a new relationship for every downstream
    workbook that uses the query.
    """

    API_BASE_URL = const.API_BASE_URL
    API_VERSION = const.API_VERSION
    CLUSTER = const.CLUSTER
    EXCLUDED_PROJECTS = const.EXCLUDED_PROJECTS
    SITE_NAME = const.SITE_NAME
    TABLEAU_BASE_URL = const.TABLEAU_BASE_URL
    TABLEAU_ACCESS_TOKEN_NAME = const.TABLEAU_ACCESS_TOKEN_NAME
    TABLEAU_ACCESS_TOKEN_SECRET = const.TABLEAU_ACCESS_TOKEN_SECRET
    VERIFY_REQUEST = const.VERIFY_REQUEST

    def init(self, conf: ConfigTree) -> None:
        self._conf = conf
        self.query = """query {
            workbooks {
                name
                projectName
                views {
                    id
                    index
                    name
                    path
                }
            }
        }"""

        self._extractor = self._build_extractor()

        transformers = []
        dict_to_model_transformer = DictToModel()
        dict_to_model_transformer.init(
            conf=Scoped.get_scoped_conf(self._conf, dict_to_model_transformer.get_scope()).with_fallback(
                ConfigFactory.from_dict(
                    {MODEL_CLASS: 'databuilder.models.dashboard.dashboard_query.DashboardQuery'})))
        transformers.append(dict_to_model_transformer)
        self._transformer = ChainedTransformer(transformers=transformers)

    def extract(self) -> Any:
        record = self._extractor.extract()
        if not record:
            return None

        return next(self._transformer.transform(record=record), None)

    def get_scope(self) -> str:
        return 'extractor.tableau_dashboard_view'

    def _build_extractor(self) -> TableauGraphQLApiViewExtractor:
        """
        Builds a TableauGraphQLApiQueryExtractor. All data required can be retrieved with a single GraphQL call.
        :return: A TableauGraphQLApiQueryExtractor that provides dashboard query metadata.
        """
        extractor = TableauGraphQLApiViewExtractor()
        tableau_extractor_conf = \
            Scoped.get_scoped_conf(self._conf, extractor.get_scope())\
                  .with_fallback(self._conf)\
                  .with_fallback(ConfigFactory.from_dict({TableauGraphQLApiExtractor.QUERY: self.query,
                                                          STATIC_RECORD_DICT: {'product': 'tableau'}
                                                          }
                                                         )
                                 )
        extractor.init(conf=tableau_extractor_conf)
        return extractor
