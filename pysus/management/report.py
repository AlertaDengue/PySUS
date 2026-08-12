"""Cross-client comparison reporting.

Given inventoried records from the three clients, produce a per-dataset
breakdown of logical files: how many exist on all three clients, on
exactly two, or on a single client (ftp-only, dadosgov-only, s3-only).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field

from .compare import Comparator
from .records import FileRecord


@dataclass
class DatasetComparison:
    dataset: str
    total: int = 0
    on_all_three: int = 0
    on_ftp_dadosgov: int = 0
    on_ftp_s3: int = 0
    on_dadosgov_s3: int = 0
    ftp_only: int = 0
    dadosgov_only: int = 0
    s3_only: int = 0
    examples: dict[str, list[str]] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "dataset": self.dataset,
            "total": self.total,
            "on_all_three": self.on_all_three,
            "on_ftp_dadosgov": self.on_ftp_dadosgov,
            "on_ftp_s3": self.on_ftp_s3,
            "on_dadosgov_s3": self.on_dadosgov_s3,
            "ftp_only": self.ftp_only,
            "dadosgov_only": self.dadosgov_only,
            "s3_only": self.s3_only,
            "examples": self.examples,
        }


class ComparisonReporter:
    """Build cross-client presence reports from inventoried records."""

    def __init__(self, comparator: Comparator | None = None):
        self.comparator = comparator or Comparator()

    def report(
        self,
        records: Iterable[FileRecord],
        example_limit: int = 3,
    ) -> list[DatasetComparison]:
        """Return one :class:`DatasetComparison` per dataset in *records*."""
        by_dataset: dict[str, list] = defaultdict(list)
        for comparison in self.comparator.compare(records):
            by_dataset[comparison.key.dataset].append(comparison)

        reports: list[DatasetComparison] = []
        for dataset, comparisons in sorted(by_dataset.items()):
            item = DatasetComparison(dataset=dataset)
            item.total = len(comparisons)

            for comparison in comparisons:
                origins = comparison.origins
                label = self._label(comparison)
                if origins == {"ftp", "dadosgov", "ducklake"}:
                    item.on_all_three += 1
                    self._example(item, "all_three", label, example_limit)
                elif origins == {"ftp", "dadosgov"}:
                    item.on_ftp_dadosgov += 1
                    self._example(item, "ftp_dadosgov", label, example_limit)
                elif origins == {"ftp", "ducklake"}:
                    item.on_ftp_s3 += 1
                elif origins == {"dadosgov", "ducklake"}:
                    item.on_dadosgov_s3 += 1
                    self._example(item, "dadosgov_s3", label, example_limit)
                elif origins == {"ftp"}:
                    item.ftp_only += 1
                    self._example(item, "ftp_only", label, example_limit)
                elif origins == {"dadosgov"}:
                    item.dadosgov_only += 1
                    self._example(item, "dadosgov_only", label, example_limit)
                elif origins == {"ducklake"}:
                    item.s3_only += 1
                    self._example(item, "s3_only", label, example_limit)

            reports.append(item)
        return reports

    @staticmethod
    def _label(comparison) -> str:
        key = comparison.key
        return (
            f"{key.dataset}/{key.group or '-'}/{key.year or '-'}/"
            f"{key.month or '-'}/{key.stem}"
        )

    @staticmethod
    def _example(
        item: DatasetComparison,
        category: str,
        label: str,
        limit: int,
    ) -> None:
        examples = item.examples.setdefault(category, [])
        if len(examples) < limit:
            examples.append(label)
