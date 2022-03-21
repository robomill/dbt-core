import os
import json
import pytest
from datetime import datetime, timedelta

import dbt.main as dbt
from aatest_simple_source import BaseSourcesTest

# put these here for now to test
class AnyStringWith:
    def __init__(self, contains=None):
        self.contains = contains

    def __eq__(self, other):
        if not isinstance(other, str):
            return False

        if self.contains is None:
            return True

        return self.contains in other

    def __repr__(self):
        return 'AnyStringWith<{!r}>'.format(self.contains)

class AnyFloat:
    """Any float. Use this in assertEqual() calls to assert that it is a float.
    """
    def __eq__(self, other):
        return isinstance(other, float)


class SuccessfulSourcesTest(BaseSourcesTest):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        self.run_dbt_with_vars(project, ['seed'])
        self.maxDiff = None
        self._id = 101
        # this is the db initial value
        self.last_inserted_time = "2016-09-19T14:45:51+00:00"
        os.environ['DBT_ENV_CUSTOM_ENV_key'] = 'value'

        yield

        del os.environ['DBT_ENV_CUSTOM_ENV_key']

    def _set_updated_at_to(self, project, delta):
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
            project.adapter.quote(c) for c in
            ('favorite_color', 'id', 'first_name',
             'email', 'ip_address', 'updated_at')
        )
        project.run_sql(
            raw_sql,
            kwargs={
                'schema': project.test_schema,
                'time': timestr,
                'id': insert_id,
                'source': project.adapter.quote('source'),
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

        # TODO: replace below calls - could they be mroe sane?
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


class TestSourceFreshness(SuccessfulSourcesTest):
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

        self._set_updated_at_to(project, timedelta(hours=-12))
        self.freshness_start_time = datetime.utcnow()
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '-o', 'target/warn_source.json'],
        )
        assert len(results) == 1
        assert results[0].status == 'warn'
        self._assert_freshness_results('target/warn_source.json', 'warn')

        self._set_updated_at_to(project, timedelta(hours=-2))
        self.freshness_start_time = datetime.utcnow()
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '-o', 'target/pass_source.json'],
        )
        assert len(results) == 1
        assert results[0].status == 'pass'
        self._assert_freshness_results('target/pass_source.json', 'pass')


class TestSourceSnapshotFreshness(SuccessfulSourcesTest):
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

        self._set_updated_at_to(project, timedelta(hours=-12))
        self.freshness_start_time = datetime.utcnow()
        results = self.run_dbt_with_vars(
            ['source', 'snapshot-freshness', '-o', 'target/warn_source.json'],
        )
        assert len(results) == 1
        assert results[0].status == 'warn'
        self._assert_freshness_results('target/warn_source.json', 'warn')

        self._set_updated_at_to(project, timedelta(hours=-2))
        self.freshness_start_time = datetime.utcnow()
        results = self.run_dbt_with_vars(
            ['source', 'snapshot-freshness', '-o', 'target/pass_source.json'],
        )
        assert len(results) == 1
        assert results[0].status == 'pass'
        self._assert_freshness_results('target/pass_source.json', 'pass')


class TestSourceFreshnessSelection(SuccessfulSourcesTest):
    def test_source_freshness_selection_select(self, project, ):
        """Tests node selection using the --select argument."""
        self._set_updated_at_to(project, timedelta(hours=-2))
        self.freshness_start_time = datetime.utcnow()
        # select source directly
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '--select',
                'source:test_source.test_table', '-o', 'target/pass_source.json'],
        )
        assert len(results) == 1
        assert results[0].status == 'pass'
        self._assert_freshness_results('target/pass_source.json', 'pass')


class TestSourceFreshnessExclude(SuccessfulSourcesTest):
    def test_source_freshness_selection_exclude(self, project, ):
        """Tests node selection using the --select argument. It 'excludes' the
        only source in the project so it should return no results."""
        self._set_updated_at_to(project, timedelta(hours=-2))
        self.freshness_start_time = datetime.utcnow()
        # exclude source directly
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '--exclude',
                'source:test_source.test_table', '-o', 'target/exclude_source.json'],
        )
        assert len(results) == 0


class TestSourceFreshnessGraph(SuccessfulSourcesTest):
    def test_source_freshness_selection_graph_operation(self, project, ):
        """Tests node selection using the --select argument with graph
        operations. `+descendant_model` == select all nodes `descendant_model`
        depends on.
        """
        self._set_updated_at_to(project, timedelta(hours=-2))
        self.freshness_start_time = datetime.utcnow()
        # select model ancestors
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '--select',
                '+descendant_model', '-o', 'target/ancestor_source.json']
        )
        assert len(results) == 1
        assert results[0].status == 'pass'
        self._assert_freshness_results('target/ancestor_source.json', 'pass')


# TODO: convert below
# class TestSourceFreshnessErrors(SuccessfulSourcesTest):
#     @property
#     def models(self):
#         return "error_models"

#     @use_profile('postgres')
#     def test_postgres_error(self):
#         results = self.run_dbt_with_vars(
#             ['source', 'freshness'],
#             expect_pass=False
#         )
#         self.assertEqual(len(results), 1)
#         self.assertEqual(results[0].status, 'runtime error')


# class TestSourceFreshnessFilter(SuccessfulSourcesTest):
#     @property
#     def models(self):
#         return 'filtered_models'

#     @use_profile('postgres')
#     def test_postgres_all_records(self):
#         # all records are filtered out
#         self.run_dbt_with_vars(
#             ['source', 'freshness'], expect_pass=False)
#         # we should insert a record with #101 that's fresh, but will still fail
#         # because the filter excludes it
#         self._set_updated_at_to(project, timedelta(hours=-2))
#         self.run_dbt_with_vars(
#             ['source', 'freshness'], expect_pass=False)

#         # we should now insert a record with #102 that's fresh, and the filter
#         # includes it
#         self._set_updated_at_to(project, timedelta(hours=-2))
#         results = self.run_dbt_with_vars(
#             ['source', 'freshness'], expect_pass=True)