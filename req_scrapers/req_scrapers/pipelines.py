# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from pathlib import Path
import csv


class ImmediateCSVPipeline:
    def __init__(self):
        self.file = None
        self.writer = None
        self.header_written = False
        self.fieldnames = None
        self.enabled = False

    def open_spider(self, spider):
        # Only enable if an explicit path is provided in settings
        output_path = spider.settings.get("IMMEDIATE_CSV_PATH")
        if not output_path:
            # No path configured; leave pipeline disabled to avoid interfering with -o feed export
            self.enabled = False
            return

        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        file_exists = path.exists() and path.stat().st_size > 0
        self.file = path.open("a", newline="", encoding="utf-8-sig")
        # Writer will be initialized on first item when we know fieldnames
        self.header_written = file_exists
        self.enabled = True

    def close_spider(self, spider):
        if self.file:
            try:
                self.file.flush()
            except Exception:
                pass
            self.file.close()
            self.file = None

    def process_item(self, item, spider):
        if not self.enabled:
            return item

        adapter = ItemAdapter(item)
        if self.writer is None:
            # Capture field order from current item
            self.fieldnames = list(adapter.asdict().keys())
            self.writer = csv.DictWriter(self.file, fieldnames=self.fieldnames)
            if not self.header_written:
                self.writer.writeheader()
                self.header_written = True

        self.writer.writerow(adapter.asdict())
        try:
            self.file.flush()
        except Exception:
            pass
        return item
