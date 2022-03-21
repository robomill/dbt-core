class TestOverrideSourceFreshness(SuccessfulSourcesTest):
    @property
    def models(self):
        return "override_freshness_models"

    @staticmethod
    def get_result_from_unique_id(data, unique_id):
        try:
            return list(filter(lambda x: x["unique_id"] == unique_id, data["results"]))[0]
        except IndexError:
            raise f"No result for the given unique_id. unique_id={unique_id}"

    def _run_override_source_freshness(self):
        self._set_updated_at_to(timedelta(hours=-30))
        self.freshness_start_time = datetime.utcnow()

        path = "target/pass_source.json"
        results = self.run_dbt_with_vars(["source", "freshness", "-o", path], expect_pass=False)
        self.assertEqual(len(results), 4)  # freshness disabled for source_e

        self.assertTrue(os.path.exists(path))
        with open(path) as fp:
            data = json.load(fp)

        result_source_a = self.get_result_from_unique_id(data, "source.test.test_source.source_a")
        self.assertEqual(result_source_a["status"], "error")
        self.assertEqual(
            result_source_a["criteria"],
            {
                "warn_after": {"count": 6, "period": "hour"},
                "error_after": {"count": 24, "period": "hour"},
                "filter": None,
            },
        )

        result_source_b = self.get_result_from_unique_id(data, "source.test.test_source.source_b")
        self.assertEqual(result_source_b["status"], "error")
        self.assertEqual(
            result_source_b["criteria"],
            {
                "warn_after": {"count": 6, "period": "hour"},
                "error_after": {"count": 24, "period": "hour"},
                "filter": None,
            },
        )

        result_source_c = self.get_result_from_unique_id(data, "source.test.test_source.source_c")
        self.assertEqual(result_source_c["status"], "warn")
        self.assertEqual(
            result_source_c["criteria"],
            {"warn_after": {"count": 6, "period": "hour"}, "error_after": None, "filter": None},
        )

        result_source_d = self.get_result_from_unique_id(data, "source.test.test_source.source_d")
        self.assertEqual(result_source_d["status"], "warn")
        self.assertEqual(
            result_source_d["criteria"],
            {
                "warn_after": {"count": 6, "period": "hour"},
                "error_after": {"count": 72, "period": "hour"},
                "filter": None,
            },
        )

    @use_profile("postgres")
    def test_postgres_override_source_freshness(self):
        self._run_override_source_freshness()
