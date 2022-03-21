import json
import os
import pytest
from datetime import datetime, timedelta

import yaml

from dbt.exceptions import ParsingException
import dbt.tracking
import dbt.version

from dbt.tests.util import run_dbt
from tests.functional.sources.fixtures import (  # noqa: F401
    error_models,
    override_freshness_models,
    models,
    malformed_models,
    filtered_models,
    macros,
    seeds,
    malformed_schema_tests,
    project_files,
    models__schema_yml,
    models__view_model_sql,
    models__ephemeral_model_sql,
    models__descendant_model_sql,
    models__multi_source_model_sql,
    models__nonsource_descendant_sql,
    seeds__source_csv,
    seeds__other_table_csv,
    seeds__expected_multi_source_csv,
    seeds__other_source_table_csv,
)


# from test.integration.base import DBTIntegrationTest, use_profile, AnyFloat, \
#     AnyStringWith


class SimpleSourcesSetup:
    @pytest.fixture(scope="class, autouse=True")
    def setup(self):
        os.environ["DBT_TEST_SCHEMA_NAME_VARIABLE"] = "test_run_schema"
        yield
        del os.environ["DBT_TEST_SCHEMA_NAME_VARIABLE"]

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models__schema_yml,
            "view_model.sql": models__view_model_sql,
            "ephemeral_model.sql": models__ephemeral_model_sql,
            "descendant_model.sql": models__descendant_model_sql,
            "multi_source_model.sql": models__multi_source_model_sql,
            "nonsource_descendant.sql": models__nonsource_descendant_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "source.csv": seeds__source_csv,
            "other_table.csv": seeds__other_table_csv,
            "expected_multi_source.csv": seeds__expected_multi_source_csv,
            "other_source_table.csv": seeds__other_source_table_csv,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "seed-paths": ["seeds"],
            "quoting": {"database": True, "schema": True, "identifier": True},
            "seeds": {
                "quote_columns": True,
            },
        }

    def run_dbt_with_vars(self, project, cmd, *args, **kwargs):
        vars_dict = {
            "test_run_schema": project.test_schema,
            "test_loaded_at": project.adapter.quote("updated_at"),
        }
        cmd.extend(["--vars", yaml.safe_dump(vars_dict)])
        return run_dbt(cmd, *args, **kwargs)

    @pytest.fixture(scope="class, autouse=True")
    def setup(self):
        super().setUp()
        self.run_dbt_with_vars(["seed"])
        self.maxDiff = None
        self._id = 101
        # this is the db initial value
        self.last_inserted_time = "2016-09-19T14:45:51+00:00"
        os.environ["DBT_ENV_CUSTOM_ENV_key"] = "value"

        yield

        del os.environ["DBT_ENV_CUSTOM_ENV_key"]

    @pytest.fixture(scope="class")
    def project_config_update(self):
        cfg = super().project_config
        cfg.update(
            {
                "macro-paths": ["macros"],
            }
        )
        return cfg

    def _create_schemas(self):
        super()._create_schemas()
        self._create_schema_named(self.default_database, self.alternative_schema())

    def alternative_schema(self):
        return self.unique_schema() + "_other"

    @pytest.fixture(scope="class, autouse=True")
    def setup(self):
        super().setUp()
        self.run_sql("create table {}.dummy_table (id int)".format(self.unique_schema()))
        self.run_sql(
            "create view {}.external_view as (select * from {}.dummy_table)".format(
                self.alternative_schema(), self.unique_schema()
            )
        )

    def run_dbt_with_vars(self, cmd, *args, **kwargs):
        vars_dict = {
            "test_run_schema": self.unique_schema(),
            "test_run_alt_schema": self.alternative_schema(),
            "test_loaded_at": self.adapter.quote("updated_at"),
        }
        cmd.extend(["--vars", yaml.safe_dump(vars_dict)])
        return run_dbt(cmd, *args, **kwargs)

    def test_basic_source_def(
        self,
        project,
    ):
        results = self.run_dbt_with_vars(["run"])
        assert len(results) == 4
        self.assertManyTablesEqual(
            ["source", "descendant_model", "nonsource_descendant"],
            ["expected_multi_source", "multi_source_model"],
        )
        results = self.run_dbt_with_vars(["test"])
        assert len(results) == 6
        print(results)

    def test_source_selector(
        self,
        project,
    ):
        # only one of our models explicitly depends upon a source
        results = self.run_dbt_with_vars(["run", "--models", "source:test_source.test_table+"])
        assert len(results) == 1
        table_comp.assertTablesEqual("source", "descendant_model")
        self.assertTableDoesNotExist("nonsource_descendant")
        self.assertTableDoesNotExist("multi_source_model")

        # do the same thing, but with tags
        results = self.run_dbt_with_vars(["run", "--models", "tag:my_test_source_table_tag+"])
        assert len(results) == 1

        results = self.run_dbt_with_vars(["test", "--models", "source:test_source.test_table+"])
        assert len(results) == 4

        results = self.run_dbt_with_vars(["test", "--models", "tag:my_test_source_table_tag+"])
        assert len(results) == 4

        results = self.run_dbt_with_vars(["test", "--models", "tag:my_test_source_tag+"])
        # test_table + other_test_table
        assert len(results) == 6

        results = self.run_dbt_with_vars(["test", "--models", "tag:id_column"])
        # all 4 id column tests
        assert len(results) == 4

    def test_empty_source_def(
        self,
        project,
    ):
        # sources themselves can never be selected, so nothing should be run
        results = self.run_dbt_with_vars(["run", "--models", "source:test_source.test_table"])
        self.assertTableDoesNotExist("nonsource_descendant")
        self.assertTableDoesNotExist("multi_source_model")
        self.assertTableDoesNotExist("descendant_model")
        assert len(results) == 0

    def test_source_only_def(
        self,
        project,
    ):
        results = self.run_dbt_with_vars(["run", "--models", "source:other_source+"])
        assert len(results) == 1
        table_comp.assertTablesEqual("expected_multi_source", "multi_source_model")
        self.assertTableDoesNotExist("nonsource_descendant")
        self.assertTableDoesNotExist("descendant_model")

        results = self.run_dbt_with_vars(["run", "--models", "source:test_source+"])
        assert len(results) == 2
        self.assertManyTablesEqual(
            ["source", "descendant_model"], ["expected_multi_source", "multi_source_model"]
        )
        self.assertTableDoesNotExist("nonsource_descendant")

    def test_source_childrens_parents(
        self,
        project,
    ):
        results = self.run_dbt_with_vars(["run", "--models", "@source:test_source"])
        assert len(results) == 2
        self.assertManyTablesEqual(
            ["source", "descendant_model"],
            ["expected_multi_source", "multi_source_model"],
        )
        self.assertTableDoesNotExist("nonsource_descendant")

    def test_run_operation_source(
        self,
        project,
    ):
        kwargs = '{"source_name": "test_source", "table_name": "test_table"}'
        self.run_dbt_with_vars(["run-operation", "vacuum_source", "--args", kwargs])

    ## this is where source_freshness_tests were

    # even seeds should fail, because parsing is what's raising
    @pytest.fixture(scope="class")
    def model_path(self):
        return "malformed_models"

    def test_malformed_schema_will_break_run(
        self,
        project,
    ):
        with pytest.raises(ParsingException):
            self.run_dbt_with_vars(["seed"])

    @pytest.fixture(scope="class")
    def model_path(self):
        return "malformed_schema_tests"

    def test_render_in_source_tests(
        self,
        project,
    ):
        self.run_dbt_with_vars(["seed"])
        self.run_dbt_with_vars(["run"])
        # syntax error at or near "{", because the test isn't rendered
        self.run_dbt_with_vars(["test"], expect_pass=False)

    @pytest.fixture(scope="class")
    def project_config_update(self):
        cfg = super().project_config
        cfg["quoting"] = {
            "identifier": False,
            "schema": False,
            "database": False,
        }
        return cfg

    def test_catalog(
        self,
        project,
    ):
        self.run_dbt_with_vars(["run"])
        self.run_dbt_with_vars(["docs", "generate"])
