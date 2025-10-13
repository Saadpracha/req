# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from pathlib import Path
import csv
import json
import importlib.util
import traceback


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


class AIEnrichmentPipeline:
    def __init__(self):
        self.enabled = False
        self._enrich_fn = None

    def open_spider(self, spider):
        # Only enable when spider.use_ai flag is true
        self.enabled = bool(getattr(spider, "use_ai", False))
        if not self.enabled:
            return
        # Attempt to import enrichment function from either package or project path
        try:
            from pathlib import Path as _Path
            ai_path_pkg = _Path.cwd() / "req_scrapers" / "req_scrapers" / "ai_enhancment.py"
            ai_path_root = _Path.cwd() / "req_scrapers" / "ai_enhancment.py"
            candidate = ai_path_pkg if ai_path_pkg.exists() else ai_path_root
            if candidate.exists():
                spec = importlib.util.spec_from_file_location("ai_enhancment_module", str(candidate))
                if spec and spec.loader:
                    mod = importlib.util.module_from_spec(spec)
                    try:
                        spec.loader.exec_module(mod)
                    except BaseException as e:
                        self._enrich_fn = None
                        try:
                            spider.errors.append(f"AI module import failed (pipeline): {e}")
                        except Exception:
                            pass
                    else:
                        self._enrich_fn = getattr(mod, "enrich_company", None)
                        if not callable(self._enrich_fn):
                            self._enrich_fn = None
                            try:
                                spider.errors.append("AI enrich_company not callable (pipeline)")
                            except Exception:
                                pass
        except BaseException as e:
            self._enrich_fn = None
            try:
                spider.errors.append(f"Failed to load AI enrichment (pipeline): {e}")
            except Exception:
                pass

    def process_item(self, item, spider):
        if not self.enabled or not callable(self._enrich_fn):
            return item

        adapter = ItemAdapter(item)
        # Build company payload from current item fields
        company_payload = {
            "nom": adapter.get("company", "") or "",
            "adresse": adapter.get("address", "") or "",
            "ville": adapter.get("city", "") or "",
            "province": adapter.get("state", "") or "",
            "code_postal": adapter.get("postal_code", "") or "",
        }

        try:
            enriched = self._enrich_fn(company_payload) or {}
        except BaseException as e:
            try:
                spider.errors.append(f"AI enrichment failed (pipeline) for NEQ {adapter.get('NEQ', '')}: {e}")
            except Exception:
                pass
            # proceed without enrichment
            return item

        # Map enrichment onto standardized fields
        contacts = enriched.get("contacts") or []
        first_contact = contacts[0] if contacts else {}

        adapter["phone"] = enriched.get("phone_number", "") or ""
        adapter["website"] = enriched.get("company_website", "") or ""
        adapter["phone_source"] = enriched.get("phone_number_source", "") or ""
        adapter["reliability"] = enriched.get("reliability_level", "") or ""
        adapter["first_name"] = first_contact.get("first_name", "") or ""
        adapter["last_name"] = first_contact.get("last_name", "") or ""
        adapter["title"] = first_contact.get("title", "") or ""
        adapter["contact_source"] = first_contact.get("source", "") or ""
        try:
            adapter["all_contacts"] = json.dumps(contacts, ensure_ascii=False)
        except Exception:
            adapter["all_contacts"] = "[]"
        adapter["note"] = enriched.get("notes", "") or ""

        return item
