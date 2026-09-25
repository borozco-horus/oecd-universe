from __future__ import annotations

from oecd_universe.discovery.catalog import parse_dbnomics_documents, parse_oecd_dataflows


def test_oecd_catalog_and_bulk_parsing() -> None:
    xml = b"""<?xml version='1.0'?>
    <message:Structure xmlns:message='http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message'
      xmlns:structure='http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure'
      xmlns:common='http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'>
      <message:Structures><structure:Dataflows><structure:Dataflow id='DSD_X@DF_X'
        agencyID='OECD.TEST' version='1.2'><common:Name xml:lang='en'>Example</common:Name>
        <common:Description xml:lang='en'>Description</common:Description><common:Annotations>
        <common:Annotation><common:AnnotationType>EXT_RESOURCE</common:AnnotationType>
        <common:AnnotationText xml:lang='en'>Example Parquet|https://example.test/x.parquet</common:AnnotationText>
        </common:Annotation></common:Annotations></structure:Dataflow></structure:Dataflows>
      </message:Structures></message:Structure>"""
    rows, resources = parse_oecd_dataflows(xml)
    assert rows[0]["dataset_id"] == "DSD_X@DF_X"
    assert rows[0]["bulk_available"] is True
    assert resources[0]["format"] == "parquet"


def test_dbnomics_dimension_and_mexico_preservation() -> None:
    docs = [
        {
            "code": "DSD_X@DF_X",
            "name": "Example",
            "description": "Example data",
            "dimensions_codes_order": ["REF_AREA", "FREQ", "MEASURE"],
            "dimensions_labels": {"REF_AREA": "Reference area", "FREQ": "Frequency"},
            "dimensions_values_labels": {
                "REF_AREA": {"MEX": "Mexico", "USA": "United States"},
                "FREQ": {"A": "Annual"},
                "MEASURE": {"TOT": "Dataset-specific total"},
            },
            "nb_series": 2,
            "dir_hash": "abc",
            "indexed_at": "2026-01-01",
        }
    ]
    datasets, dimensions, codelists = parse_dbnomics_documents(docs)
    assert datasets[0]["contains_mexico"] is True
    assert datasets[0]["frequency_codes"] == "A"
    assert len(dimensions) == 3
    assert any(row["dataset_id"] == "DSD_X@DF_X" and row["code"] == "TOT" for row in codelists)
