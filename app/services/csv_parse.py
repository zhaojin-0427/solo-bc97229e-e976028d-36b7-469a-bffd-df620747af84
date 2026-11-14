"""Parsing and validation for the two uploaded CSV flavours.

Uploaded files are untrusted: every row is fully validated before any
database write happens, so a bad file can never partially land ("整批提交").
"""
from __future__ import annotations

import csv
import io
from dataclasses import dataclass, field
from typing import List, Tuple

from ..errors import CsvValidationError

RELATION_HEADERS = ({"原样本号", "衍生样本号"}, {"parent_sample", "child_sample"})
GROUP_HEADERS = ({"样本号", "组别"}, {"sample_no", "grp"}, {"sample_no", "group"})


@dataclass
class RelationBatch:
    edges: List[Tuple[str, str]] = field(default_factory=list)
    new_count: int = 0          # filled by the repository against current rows
    duplicate_count: int = 0


@dataclass
class GroupBatch:
    assignments: List[Tuple[str, str]] = field(default_factory=list)
    duplicate_count: int = 0


def _decode(raw: bytes, max_bytes: int) -> str:
    if not raw:
        raise CsvValidationError("上传文件为空")
    if len(raw) > max_bytes:
        raise CsvValidationError(
            f"文件超过大小上限 {max_bytes} 字节（实际 {len(raw)} 字节）"
        )
    try:
        # utf-8-sig transparently strips a BOM if the LIMS export includes one.
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CsvValidationError(f"文件不是合法 UTF-8 编码: {exc}") from exc


def _read_rows(raw: bytes, max_bytes: int) -> Tuple[List[str], List[dict]]:
    text = _decode(raw, max_bytes)
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        raise CsvValidationError("CSV 无任何内容")
    header = [h.strip() for h in rows[0]]
    if any(h == "" for h in header):
        raise CsvValidationError(f"表头存在空列名: {header}")
    dict_rows: List[dict] = []
    for line_no, row in enumerate(rows[1:], start=2):
        if not row or all(cell.strip() == "" for cell in row):
            continue  # tolerate blank lines
        if len(row) != len(header):
            raise CsvValidationError(
                f"第 {line_no} 行列数 {len(row)} 与表头列数 {len(header)} 不一致"
            )
        dict_rows.append(
            {header[i]: row[i].strip() for i in range(len(header))}
            | {"_line": line_no}
        )
    if not dict_rows:
        raise CsvValidationError("CSV 只有表头，没有数据行")
    return header, dict_rows


def _match_header(header: List[str], candidates) -> Tuple[str, str]:
    head_set = set(header)
    for candidate in candidates:
        if head_set >= candidate:
            cols = [h for h in header if h in candidate]
            return cols[0], cols[1]
    raise CsvValidationError(
        f"表头 {header} 不符合要求，期望包含 {sorted(candidates[0])} 两列"
    )


def parse_relations(raw: bytes, max_bytes: int) -> List[Tuple[str, str]]:
    """Parse a LIMS derivation CSV into (parent, child) tuples."""
    header, rows = _read_rows(raw, max_bytes)
    col_a, col_b = _match_header(header, RELATION_HEADERS)
    edges: List[Tuple[str, str]] = []
    seen = set()
    for row in rows:
        a, b = row[col_a], row[col_b]
        if not a or not b:
            raise CsvValidationError(
                f"第 {row['_line']} 行存在空样本号: {row[col_a]!r}, {row[col_b]!r}"
            )
        edge = (a, b)
        if edge not in seen:
            seen.add(edge)
            edges.append(edge)
    return edges


def parse_groups(raw: bytes, max_bytes: int) -> GroupBatch:
    """Parse a grouping CSV. One sample may only carry one group."""
    header, rows = _read_rows(raw, max_bytes)
    col_sample, col_group = _match_header(header, GROUP_HEADERS)
    assignments: List[Tuple[str, str]] = []
    sample_to_group: dict[str, str] = {}
    duplicate_count = 0
    for row in rows:
        sample, grp = row[col_sample], row[col_group]
        if not sample or not grp:
            raise CsvValidationError(
                f"第 {row['_line']} 行存在空样本号或空组别"
            )
        if sample in sample_to_group:
            if sample_to_group[sample] != grp:
                raise CsvValidationError(
                    f"同一样本 {sample} 在文件中被分到多个组: "
                    f"{sample_to_group[sample]} / {grp}"
                )
            duplicate_count += 1  # identical repeated row, harmless
            continue
        sample_to_group[sample] = grp
        assignments.append((sample, grp))
    return GroupBatch(assignments=assignments, duplicate_count=duplicate_count)
