import json
import os
from datetime import datetime, timedelta
import pytest
import yaml

from dbt.exceptions import ParsingException
import dbt.tracking
import dbt.version

from dbt.tests.util import run_dbt
from tests.functional.sources.fixtures import error_models,override_freshness_models,models,malformed_models,filtered_models,macros,seeds,malformed_schema_tests,project_files # noqa: F401

# from test.integration.base import DBTIntegrationTest, use_profile, AnyFloat, \
#     AnyStringWith


    def _set_updated_at_to(self, delta):
        insert_time = datetime.utcnow() + delta
        timestr = insert_time.strftime("%Y-%m-%d %H:%M:%S")
        # favorite_color,id,first_name,email,ip_address,updated_at
        insert_id = self._id
        self._id += 1
        raw_sql = """INSERT INTO {schema}.{source}
            ({quoted_columns})
        VALUES (
            'blue',{id},'Jake','abc@example.com','192.168.1.1','{time}'
        )"""
        quoted_columns = ','.join(
            self.adapter.quote(c) for c in
            ('favorite_color', 'id', 'first_name',
             'email', 'ip_address', 'updated_at')
        )
        self.run_sql(
            raw_sql,
            kwargs={
                'schema': self.unique_schema(),
                'time': timestr,
                'id': insert_id,
                'source': self.adapter.quote('source'),
                'quoted_columns': quoted_columns,
            }
        )
        self.last_inserted_time = insert_time.strftime(
            "%Y-%m-%dT%H:%M:%S+00:00")
    
    
    
    def _assert_freshness_results(self, path, state):
        assert os.path.exists(path)
        with open(path) as fp:
            data = json.load(fp)

        assert set(data) == {'metadata', 'results', 'elapsed_time'}
        assert 'generated_at' in data['metadata']
        assert isinstance(data['elapsed_time'], float)
        self.assertBetween(data['metadata']['generated_at'],
                           self.freshness_start_time)
        assert data['metadata']['dbt_schema_version'] == 'https://schemas.getdbt.com/dbt/sources/v3.json'
        assert data['metadata']['dbt_version'] == dbt.version.__version__
        assert data['metadata']['invocation_id'] == dbt.tracking.active_user.invocation_id
        key = 'key'
        if os.name == 'nt':
            key = key.upper()
        assert data['metadata']['env'] == {key: 'value'}

        last_inserted_time = self.last_inserted_time

        assert len(data['results']) == 1

        assert data['results'] == [
            {
                'unique_id': 'source.test.test_source.test_table',
                'max_loaded_at': last_inserted_time,
                'snapshotted_at': AnyStringWith(),
                'max_loaded_at_time_ago_in_s': AnyFloat(),
                'status': state,
                'criteria': {
                    'filter': None,
                    'warn_after': {'count': 10, 'period': 'hour'},
                    'error_after': {'count': 18, 'period': 'hour'},
                },
                'adapter_response': {},
                'thread_id': AnyStringWith('Thread-'),
                'execution_time': AnyFloat(),
                'timing': [
                    {
                        'name': 'compile',
                        'started_at': AnyStringWith(),
                        'completed_at': AnyStringWith(),
                    },
                    {
                        'name': 'execute',
                        'started_at': AnyStringWith(),
                        'completed_at': AnyStringWith(),
                    }
                ]
            }
        ]


    def test_source_freshness(self, project):
        # test_source.test_table should have a loaded_at field of `updated_at`
        # and a freshness of warn_after: 10 hours, error_after: 18 hours
        # by default, our data set is way out of date!
        self.freshness_start_time = datetime.utcnow()
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '-o', 'target/error_source.json'],
            expect_pass=False
        )
        assert len(results) == 1
        assert results[0].status == 'error'
        self._assert_freshness_results('target/error_source.json', 'error')

        self._set_updated_at_to(timedelta(hours=-12))
        self.freshness_start_time = datetime.utcnow()
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '-o', 'target/warn_source.json'],
        )
        assert len(results) == 1
        assert results[0].status == 'warn'
        self._assert_freshness_results('target/warn_source.json', 'warn')

        self._set_updated_at_to(timedelta(hours=-2))
        self.freshness_start_time = datetime.utcnow()
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '-o', 'target/pass_source.json'],
        )
        assert len(results) == 1
        assert results[0].status == 'pass'
        self._assert_freshness_results('target/pass_source.json', 'pass')


    def test_source_snapshot_freshness(self, project, ):
        """Ensures that the deprecated command `source snapshot-freshness`
        aliases to `source freshness` command.
        """
        self.freshness_start_time = datetime.utcnow()
        results = self.run_dbt_with_vars(
            ['source', 'snapshot-freshness', '-o', 'target/error_source.json'],
            expect_pass=False
        )
        assert len(results) == 1
        assert results[0].status == 'error'
        self._assert_freshness_results('target/error_source.json', 'error')

        self._set_updated_at_to(timedelta(hours=-12))
        self.freshness_start_time = datetime.utcnow()
        results = self.run_dbt_with_vars(
            ['source', 'snapshot-freshness', '-o', 'target/warn_source.json'],
        )
        assert len(results) == 1
        assert results[0].status == 'warn'
        self._assert_freshness_results('target/warn_source.json', 'warn')

        self._set_updated_at_to(timedelta(hours=-2))
        self.freshness_start_time = datetime.utcnow()
        results = self.run_dbt_with_vars(
            ['source', 'snapshot-freshness', '-o', 'target/pass_source.json'],
        )
        assert len(results) == 1
        assert results[0].status == 'pass'
        self._assert_freshness_results('target/pass_source.json', 'pass')

    def test_source_freshness_selection_select(self, project, ):
        """Tests node selection using the --select argument."""
        self._set_updated_at_to(timedelta(hours=-2))
        self.freshness_start_time = datetime.utcnow()
        # select source directly
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '--select',
                'source:test_source.test_table', '-o', 'target/pass_source.json'],
        )
        assert len(results) == 1
        assert results[0].status == 'pass'
        self._assert_freshness_results('target/pass_source.json', 'pass')

    def test_source_freshness_selection_exclude(self, project, ):
        """Tests node selection using the --select argument. It 'excludes' the
        only source in the project so it should return no results."""
        self._set_updated_at_to(timedelta(hours=-2))
        self.freshness_start_time = datetime.utcnow()
        # exclude source directly
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '--exclude',
                'source:test_source.test_table', '-o', 'target/exclude_source.json'],
        )
        assert len(results) == 0

    def test_source_freshness_selection_graph_operation(self, project, ):
        """Tests node selection using the --select argument with graph
        operations. `+descendant_model` == select all nodes `descendant_model`
        depends on.
        """
        self._set_updated_at_to(timedelta(hours=-2))
        self.freshness_start_time = datetime.utcnow()
        # select model ancestors
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '--select',
                '+descendant_model', '-o', 'target/ancestor_source.json']
        )
        assert len(results) == 1
        assert results[0].status == 'pass'
        self._assert_freshness_results('target/ancestor_source.json', 'pass')


    @pytest.fixture(scope="class")
    def model_path(self):
        return "override_freshness_models"
    
    def get_result_from_unique_id(data, unique_id):
        try:
            return list(filter(lambda x : x['unique_id'] == unique_id,  data['results']))[0]
        except IndexError:
            raise f"No result for the given unique_id. unique_id={unique_id}"

    def _run_override_source_freshness(self):
        self._set_updated_at_to(timedelta(hours=-30))
        self.freshness_start_time = datetime.utcnow()

        path = 'target/pass_source.json'
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '-o', path],
            expect_pass=False
        )
        assert len(results) == 4 # freshness disabled for source_e       

        assert os.path.exists(path)
        with open(path) as fp:
            data = json.load(fp)

        result_source_a = self.get_result_from_unique_id(data, 'source.test.test_source.source_a')
        assert result_source_a['status'] == 'error'
        assert result_source_a['criteria'] == \
            {
                'warn_after': {'count': 6, 'period': 'hour'},
                'error_after': {'count': 24, 'period': 'hour'},
                'filter': None
            }

        result_source_b = self.get_result_from_unique_id(data, 'source.test.test_source.source_b')
        assert result_source_b['status'] == 'error'
        assert result_source_b['criteria'] == \
            {
                'warn_after': {'count': 6, 'period': 'hour'},
                'error_after': {'count': 24, 'period': 'hour'},
                'filter': None
            }

        result_source_c = self.get_result_from_unique_id(data, 'source.test.test_source.source_c')
        assert result_source_c['status'] == 'warn'
        assert result_source_c['criteria'] == \
            {
                'warn_after': {'count': 6, 'period': 'hour'},
                'error_after': None,
                'filter': None
            }

        result_source_d = self.get_result_from_unique_id(data, 'source.test.test_source.source_d')
        assert result_source_d['status'] == 'warn'
        assert result_source_d['criteria'] == \
            {
                'warn_after': {'count': 6, 'period': 'hour'},
                'error_after': {'count': 72, 'period': 'hour'},
                'filter': None
            }

    def test_override_source_freshness(self, project, ):
        self._run_override_source_freshness()

    @pytest.fixture(scope="class")
    def model_path(self):
        return "error_models"

    def test_error(self, project, ):
        results = self.run_dbt_with_vars(
            ['source', 'freshness'],
            expect_pass=False
        )
        assert len(results) == 1
        assert results[0].status == 'runtime error'


    @pytest.fixture(scope="class")
    def model_path(self):
        return 'filtered_models'

    def test_all_records(self, project, ):
        # all records are filtered out
        self.run_dbt_with_vars(
            ['source', 'freshness'], expect_pass=False)
        # we should insert a record with #101 that's fresh, but will still fail
        # because the filter excludes it
        self._set_updated_at_to(timedelta(hours=-2))
        self.run_dbt_with_vars(
            ['source', 'freshness'], expect_pass=False)

        # we should now insert a record with #102 that's fresh, and the filter
        # includes it
        self._set_updated_at_to(timedelta(hours=-2))
        results = self.run_dbt_with_vars(
            ['source', 'freshness'], expect_pass=True)