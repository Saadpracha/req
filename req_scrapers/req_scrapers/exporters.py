# exporters.py
import csv
from scrapy.exporters import CsvItemExporter


class QuotedCsvItemExporter(CsvItemExporter):
    """
    Custom CSV exporter that forces quotes around all fields.
    """
    def __init__(self, file, **kwargs):
        kwargs.setdefault("quoting", csv.QUOTE_ALL)
        super().__init__(file, **kwargs)
