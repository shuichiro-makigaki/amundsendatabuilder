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


class TableauGraphQLApiOwnerExtractor(TableauGraphQLApiExtractor):
    """
    Implements the extraction-time logic for parsing the GraphQL result and transforming into a dict
    that fills the DashboardMetadata model. Allows workbooks to be exlcuded based on their project.
    """

    CLUSTER = const.CLUSTER
    EXCLUDED_PROJECTS = const.EXCLUDED_PROJECTS
    TABLEAU_BASE_URL = const.TABLEAU_BASE_URL

    def execute(self) -> Iterator[Dict[str, Any]]:
        response = self.execute_query()
        workbooks_data = [workbook for workbook in response['workbooks']
                          if workbook['projectName'] not in
                          self._conf.get_list(TableauGraphQLApiOwnerExtractor.EXCLUDED_PROJECTS)]
        for workbook in workbooks_data:
            if None in (workbook['projectName'], workbook['name']):
                LOGGER.warning(f'Ignoring workbook (ID:{workbook["vizportalUrlId"]}) ' +
                               f'in project (ID:{workbook["projectVizportalUrlId"]}) because of a lack of permission')
                continue
            data = {
                'cluster': self._conf.get_string(TableauGraphQLApiOwnerExtractor.CLUSTER),
                'dashboard_group_id': workbook['projectName'],
                'dashboard_id': TableauDashboardUtils.sanitize_workbook_name(workbook['name']),
                'email': workbook['owner']['email'],
            }
            yield data


class TableauDashboardOwnerExtractor(Extractor):
    """
    Extracts core metadata about Tableau "dashboards".
    For the purposes of this extractor, Tableau "workbooks" are mapped to Amundsen dashboards, and the
    top-level project in which these workbooks preside is the dashboard group. The metadata it gathers is:
        Dashboard name (Workbook name)
        Dashboard description (Workbook description)
        Dashboard creation timestamp (Workbook creationstamp)
        Dashboard group name (Workbook top-level folder name)
    Uses the Metadata API: https://help.tableau.com/current/api/metadata_api/en-us/index.html
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
                owner {
                    email
                }
                projectVizportalUrlId
                vizportalUrlId
            }
        }"""

        self._extractor = self._build_extractor()

        dict_to_model_transformer = DictToModel()
        dict_to_model_transformer.init(
            conf=Scoped.get_scoped_conf(self._conf, dict_to_model_transformer.get_scope()).with_fallback(
                ConfigFactory.from_dict(
                    {MODEL_CLASS: 'databuilder.models.dashboard.dashboard_owner.DashboardOwner'})))
        self._transformer = ChainedTransformer(transformers=[dict_to_model_transformer])

    def extract(self) -> Any:
        record = self._extractor.extract()
        if not record:
            return None
        return next(self._transformer.transform(record=record), None)

    def get_scope(self) -> str:
        return 'extractor.tableau_dashboard_owner'

    def _build_extractor(self) -> TableauGraphQLApiOwnerExtractor:
        """
        Builds a TableauGraphQLApiMetadataExtractor. All data required can be retrieved with a single GraphQL call.
        :return: A TableauGraphQLApiMetadataExtractor that provides core dashboard metadata.
        """
        extractor = TableauGraphQLApiOwnerExtractor()
        extractor.init(
            conf=Scoped.get_scoped_conf(
                self._conf, extractor.get_scope()
            ).with_fallback(
                self._conf
            ).with_fallback(
                ConfigFactory.from_dict({
                    TableauGraphQLApiExtractor.QUERY: self.query,
                    STATIC_RECORD_DICT: {'product': 'tableau'}
                })
            )
        )
        return extractor


class TableauGraphQLApiUserExtractor(TableauGraphQLApiExtractor):
    """
    Implements the extraction-time logic for parsing the GraphQL result and transforming into a dict
    that fills the DashboardMetadata model. Allows workbooks to be exlcuded based on their project.
    """

    CLUSTER = const.CLUSTER
    EXCLUDED_PROJECTS = const.EXCLUDED_PROJECTS
    TABLEAU_BASE_URL = const.TABLEAU_BASE_URL

    def execute(self) -> Iterator[Dict[str, Any]]:
        for workbook in self.execute_query()['workbooks']:
            yield {
                'email': workbook['owner']['email'],
                'full_name': workbook['owner']['name'],
                'first_name': workbook['owner']['username']
            }


class TableauDashboardUserExtractor(Extractor):
    """
    Extracts core metadata about Tableau "dashboards".
    For the purposes of this extractor, Tableau "workbooks" are mapped to Amundsen dashboards, and the
    top-level project in which these workbooks preside is the dashboard group. The metadata it gathers is:
        Dashboard name (Workbook name)
        Dashboard description (Workbook description)
        Dashboard creation timestamp (Workbook creationstamp)
        Dashboard group name (Workbook top-level folder name)
    Uses the Metadata API: https://help.tableau.com/current/api/metadata_api/en-us/index.html
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
                owner {
                    username
                    name
                    email
                }
            }
        }"""
        self._extractor = TableauGraphQLApiUserExtractor()
        self._extractor.init(
            conf=Scoped.get_scoped_conf(
                self._conf, self._extractor.get_scope()
            ).with_fallback(
                self._conf
            ).with_fallback(
                ConfigFactory.from_dict({
                    TableauGraphQLApiExtractor.QUERY: self.query,
                    STATIC_RECORD_DICT: {'product': 'tableau'}
                })
            )
        )

        dict_to_model_transformer = DictToModel()
        dict_to_model_transformer.init(
            conf=Scoped.get_scoped_conf(
                self._conf, dict_to_model_transformer.get_scope()
            ).with_fallback(
                ConfigFactory.from_dict({MODEL_CLASS: 'databuilder.models.user.User'})
            )
        )
        self._transformer = ChainedTransformer(transformers=[dict_to_model_transformer])

    def extract(self) -> Any:
        record = self._extractor.extract()
        if not record:
            return None
        record = next(self._transformer.transform(record=record), None)
        return record

    def get_scope(self) -> str:
        return 'extractor.tableau_dashboard_user'
