# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


from itemadapter import ItemAdapter  # useful for handling different item types with a single interface
from pathlib import Path
import csv
import json
import importlib.util
import os
import traceback

import pymysql


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
            self.writer = csv.DictWriter(self.file, fieldnames=self.fieldnames, quoting=csv.QUOTE_ALL)
            if not self.header_written:
                self.writer.writeheader()
                self.header_written = True

        self.writer.writerow(adapter.asdict())
        try:
            self.file.flush()
        except Exception:
            pass
        return item


class MySQLCtqPipeline:
    """
    Store CTQ items into MySQL table `ctq`.

    - Uses UNIQUE KEY on `neq` with ON DUPLICATE KEY UPDATE so re-runs refresh data.
    - If DB connection fails, the pipeline disables itself and items continue
      through the normal file/feed exporters (fallback logic).
    - Connection details are taken from Scrapy settings first, then fall back
      to environment variables, and finally to hardcoded defaults.
    """

    def __init__(self, host, db, user, password, port=3306):
        self.host = host
        self.db = db
        self.user = user
        self.password = password
        self.port = int(port) if port else 3306
        self.conn = None
        self.cursor = None
        self.enabled = False

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings

        host = settings.get("MYSQL_HOST") or os.getenv("MYSQL_HOST") or "52.60.176.24"
        db = settings.get("MYSQL_DB") or os.getenv("MYSQL_DB") or "v1lead2424_REQ_DB"
        user = settings.get("MYSQL_USER") or os.getenv("MYSQL_USER") or "v1lead2424_saad"
        password = settings.get("MYSQL_PASSWORD") or os.getenv("MYSQL_PASSWORD") or "1g2jWMb$WE^7HLe0"
        port = settings.getint("MYSQL_PORT", int(os.getenv("MYSQL_PORT", "3306")))

        return cls(host=host, db=db, user=user, password=password, port=port)

    def open_spider(self, spider):
        try:
            self.conn = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.db,
                port=self.port,
                charset="utf8mb4",
                autocommit=False,
                cursorclass=pymysql.cursors.DictCursor,
            )
            self.cursor = self.conn.cursor()
            self.enabled = True
            spider.logger.info(
                f"MySQLCtqPipeline: connected to {self.host}:{self.port}/{self.db}"
            )
        except Exception as e:
            # Disable pipeline but allow spider to continue with file / feed exporters
            self.enabled = False
            spider.logger.error(f"MySQLCtqPipeline: failed to connect to DB: {e}")

    def close_spider(self, spider):
        try:
            if self.conn:
                try:
                    self.conn.commit()
                except Exception:
                    pass
                self.cursor.close()
                self.conn.close()
        except Exception:
            pass
        finally:
            self.conn = None
            self.cursor = None
            self.enabled = False

    def process_item(self, item, spider):
        if not self.enabled:
            return item

        adapter = ItemAdapter(item)

        # Map item fields to ctq table columns
        telephone = (
            adapter.get("telephone")
            or adapter.get("phone")
            or adapter.get("telephone_number")
        )

        sql = """
            INSERT INTO ctq (
                neq,
                nom,
                nir,
                titre,
                full_address,
                adresse,
                ville,
                province,
                code_postal,
                pays,
                telephone,
                categorie_transport,
                date_inscription,
                date_prochaine_maj,
                code_securite,
                droit_circulation,
                droit_exploiter,
                motif,
                extra_values,
                vrac_numero_inscription,
                vrac_region_exploitation,
                vrac_nombre_camions,
                vrac_nom_courtier
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                nom = VALUES(nom),
                nir = VALUES(nir),
                titre = VALUES(titre),
                full_address = VALUES(full_address),
                adresse = VALUES(adresse),
                ville = VALUES(ville),
                province = VALUES(province),
                code_postal = VALUES(code_postal),
                pays = VALUES(pays),
                telephone = VALUES(telephone),
                categorie_transport = VALUES(categorie_transport),
                date_inscription = VALUES(date_inscription),
                date_prochaine_maj = VALUES(date_prochaine_maj),
                code_securite = VALUES(code_securite),
                droit_circulation = VALUES(droit_circulation),
                droit_exploiter = VALUES(droit_exploiter),
                motif = VALUES(motif),
                extra_values = VALUES(extra_values),
                vrac_numero_inscription = VALUES(vrac_numero_inscription),
                vrac_region_exploitation = VALUES(vrac_region_exploitation),
                vrac_nombre_camions = VALUES(vrac_nombre_camions),
                vrac_nom_courtier = VALUES(vrac_nom_courtier)
        """

        # Convert empty strings to None for varchar fields (cleaner than empty strings)
        vrac_nombre_camions = adapter.get("vrac_nombre_camions")
        if vrac_nombre_camions == "" or vrac_nombre_camions is None:
            vrac_nombre_camions = None

        params = (
            adapter.get("neq"),
            adapter.get("nom"),
            adapter.get("nir"),
            adapter.get("titre"),
            adapter.get("full_address"),
            adapter.get("adresse"),
            adapter.get("ville"),
            adapter.get("province"),
            adapter.get("code_postal"),
            adapter.get("pays"),
            telephone,
            adapter.get("categorie_transport"),
            adapter.get("date_inscription"),
            adapter.get("date_prochaine_maj"),
            adapter.get("code_securite"),
            adapter.get("droit_circulation"),
            adapter.get("droit_exploiter"),
            adapter.get("motif"),
            adapter.get("extra_values"),
            adapter.get("vrac_numero_inscription"),
            adapter.get("vrac_region_exploitation"),
            vrac_nombre_camions,  # varchar(10) - keep as string or NULL
            adapter.get("vrac_nom_courtier"),
        )

        try:
            self.cursor.execute(sql, params)
            # Commit every row; if you want batching, this could be optimized.
            self.conn.commit()
        except Exception as e:
            spider.logger.error(
                f"MySQLCtqPipeline: insert/update failed for NEQ={adapter.get('neq')}: {e}"
            )
            try:
                self.conn.rollback()
            except Exception:
                pass
            # Return item so file/feed fallback still works
            return item

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
