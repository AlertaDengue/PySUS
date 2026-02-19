import unittest

import pysus
from pysus.ftp import Database


class TestAvailableDatabases(unittest.TestCase):
    """Test suite for AVAILABLE_DATABASES registry"""

    def test_available_databases_exists(self):
        """Verify AVAILABLE_DATABASES is accessible from pysus namespace"""
        self.assertTrue(hasattr(pysus, "AVAILABLE_DATABASES"))
        self.assertIsInstance(pysus.AVAILABLE_DATABASES, list)

    def test_available_databases_not_empty(self):
        """Verify AVAILABLE_DATABASES list contains entries"""
        self.assertGreater(len(pysus.AVAILABLE_DATABASES), 0)

    def test_all_are_database_classes(self):
        """Verify all entries inherit from Database base class"""
        for db_class in pysus.AVAILABLE_DATABASES:
            with self.subTest(db_class=db_class):
                self.assertTrue(
                    issubclass(db_class, Database),
                    f"{db_class.__name__} does not inherit from Database",
                )

    def test_all_have_required_attributes(self):
        """Verify all database classes have name, paths, metadata"""
        for db_class in pysus.AVAILABLE_DATABASES:
            with self.subTest(db_class=db_class):
                db_instance = db_class()
                self.assertTrue(
                    hasattr(db_instance, "name"),
                    f"{db_class.__name__} missing 'name' attribute",
                )
                self.assertTrue(
                    hasattr(db_instance, "paths"),
                    f"{db_class.__name__} missing 'paths' attribute",
                )
                self.assertTrue(
                    hasattr(db_instance, "metadata"),
                    f"{db_class.__name__} missing 'metadata' attribute",
                )

    def test_all_have_valid_metadata(self):
        """Verify metadata contains required fields"""
        required_fields = {"long_name", "source", "description"}
        for db_class in pysus.AVAILABLE_DATABASES:
            with self.subTest(db_class=db_class):
                db_instance = db_class()
                metadata_keys = set(db_instance.metadata.keys())
                self.assertTrue(
                    required_fields.issubset(metadata_keys),
                    f"{db_class.__name__} metadata missing required fields. "
                    f"Expected: {required_fields}, Got: {metadata_keys}",
                )
                # verify values exist (can be strings or tuples)
                for field in required_fields:
                    value = db_instance.metadata[field]
                    if field == "source":
                        # can be a string or tuple of strings
                        self.assertTrue(
                            isinstance(value, (str, tuple)),
                            f"{db_class.__name__}.metadata['source'] "
                            f"must be str or tuple",
                        )
                        if isinstance(value, tuple):
                            self.assertTrue(
                                all(isinstance(s, str) for s in value),
                                f"{db_class.__name__}.metadata['source'] "
                                f"tuple must contain only strings",
                            )
                    else:
                        # long_name and description should be strings
                        # Note: Some databases may have empty descriptions
                        self.assertIsInstance(
                            value,
                            str,
                            f"{db_class.__name__}.metadata['{field}'] "
                            f"is not a string",
                        )

    def test_expected_databases_present(self):
        """Verify all expected database classes are included"""
        expected_databases = {
            "CIHA",
            "CNES",
            "IBGEDATASUS",
            "PNI",
            "SIA",
            "SIH",
            "SIM",
            "SINAN",
            "SINASC",
        }
        actual_databases = {
            db_class.__name__ for db_class in pysus.AVAILABLE_DATABASES
        }
        self.assertEqual(
            expected_databases,
            actual_databases,
            f"Database list mismatch. "
            f"Missing: {expected_databases - actual_databases}, "
            f"Extra: {actual_databases - expected_databases}",
        )

    def test_can_instantiate_all_databases(self):
        """Verify all database classes can be instantiated without errors"""
        for db_class in pysus.AVAILABLE_DATABASES:
            with self.subTest(db_class=db_class):
                try:
                    db_instance = db_class()
                    self.assertIsInstance(db_instance, Database)
                except Exception as e:
                    self.fail(
                        f"Failed to instantiate {db_class.__name__}: {e}"
                    )

    def test_list_order_is_consistent(self):
        """Document that the list order is alphabetical by class name"""
        class_names = [
            db_class.__name__ for db_class in pysus.AVAILABLE_DATABASES
        ]
        sorted_names = sorted(class_names)
        self.assertEqual(
            class_names,
            sorted_names,
            "AVAILABLE_DATABASES should be in alphabetical order",
        )

    def test_usage_example(self):
        """Demonstrate iteration pattern for accessing metadata"""
        databases_info = []
        for db_class in pysus.AVAILABLE_DATABASES:
            db = db_class()
            databases_info.append(
                {
                    "name": db.name,
                    "long_name": db.metadata["long_name"],
                    "description": db.metadata["description"],
                }
            )

        self.assertEqual(len(databases_info), 9)

        # vrify all entries have the expected structure
        for info in databases_info:
            self.assertIn("name", info)
            self.assertIn("long_name", info)
            self.assertIn("description", info)
            self.assertTrue(isinstance(info["name"], str))
            self.assertTrue(isinstance(info["long_name"], str))
            self.assertTrue(isinstance(info["description"], str))


if __name__ == "__main__":
    unittest.main()
