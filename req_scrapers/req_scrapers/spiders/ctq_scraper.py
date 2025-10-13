import csv
import json
import base64
import uuid
import traceback
from datetime import datetime
from pathlib import Path
import importlib.util
import scrapy
from scrapy.http import FormRequest
from urllib.parse import urljoin


class CtqScraperSpider(scrapy.Spider):
    name = "ctq_scraper"
    allowed_domains = ["pes.ctq.gouv.qc.ca"]
    start_urls = ["https://www.pes.ctq.gouv.qc.ca/pes2/mvc/dossierclient"]

    # proxy state
    proxy_list = []
    current_proxy_index = 0

    def __init__(self, neqs=None, file=None, source=None, ai=None, ai_count=None, summary_file=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.neqs = neqs.split(",") if neqs else []
        if file:
            # Resolve file path relative to current working directory
            file_path = Path(file)
            if not file_path.is_absolute():
                # Try current directory first, then parent directory
                current_path = Path.cwd() / file
                parent_path = Path.cwd().parent / file
                if current_path.exists():
                    file_path = current_path
                elif parent_path.exists():
                    file_path = parent_path
                else:
                    file_path = current_path  # Use current path for error reporting
            loaded_neqs = self._load_neqs_from_file(str(file_path))
            self.neqs.extend(loaded_neqs)
            print(f"DEBUG: Loaded {len(loaded_neqs)} NEQs from file: {file_path}")
        self.neqs = [str(n).strip() for n in self.neqs if str(n).strip()]
        print(f"DEBUG: Total NEQs to process: {len(self.neqs)}")

        # run metadata
        self.source = source or ""
        self.run_id = uuid.uuid4().hex
        self.start_time = None
        self.end_time = None
        self.errors = []
        self._enrich_fn = None
        
        # AI enhancement settings
        ai_str = str(ai).strip().lower() if ai is not None else "yes"
        self.use_ai = ai_str in ("yes", "true", "1")
        
        # AI count limit
        self.ai_count_limit = None
        if ai_count:
            try:
                self.ai_count_limit = int(ai_count)
                if self.ai_count_limit <= 0:
                    self.ai_count_limit = None
            except ValueError:
                self.ai_count_limit = None
        
        # Summary file
        self.summary_file = summary_file or f"ctq_summary_{self.run_id}.json"
        
        # Statistics tracking
        self.stats = {
            "total_processed": 0,
            "ai_enhanced": 0,
            "skipped_droit_circulation": 0,
            "ai_limit_reached": False,
            "errors": [],
            "invalid_phone_skipped": 0
        }

        # Load proxies from proxies.json at project root if present
        try:
            proxies_path = Path.cwd() / "proxies.json"
            if proxies_path.is_file():
                with proxies_path.open("r", encoding="utf-8-sig") as pf:
                    loaded = json.load(pf)
                    if isinstance(loaded, list):
                        self.proxy_list = [str(x).strip() for x in loaded if str(x).strip()]
                        if self.proxy_list:
                            self.logger.info(f"Loaded {len(self.proxy_list)} proxies from {proxies_path}")
                    else:
                        self.logger.warning("proxies.json content is not a list; skipping proxies load")
            else:
                self.logger.info("proxies.json not found; running without proxies")
        except Exception as e:
            self.logger.warning(f"Failed to load proxies.json: {e}")

    def open_spider(self, spider):
        # record start time and prepare AI enrichment function if available
        self.start_time = datetime.utcnow()
        self.logger.info(f"CTQ Scraper started at {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"Processing {len(self.neqs)} NEQ numbers")
        if self.ai_count_limit:
            self.logger.info(f"AI enhancement limited to {self.ai_count_limit} companies")
        if not self.use_ai:
            self.logger.info("AI enrichment disabled via ai flag")
            return
        try:
            # Preferred path inside package: req_scrapers/req_scrapers/ai_enhancment.py
            ai_path_pkg = Path.cwd() / "req_scrapers" / "req_scrapers" / "ai_enhancment.py"
            ai_path_root = Path.cwd() / "req_scrapers" / "ai_enhancment.py"
            candidate = ai_path_pkg if ai_path_pkg.exists() else ai_path_root
            if candidate.exists():
                spec = importlib.util.spec_from_file_location("ai_enhancment_module", str(candidate))
                if spec and spec.loader:
                    mod = importlib.util.module_from_spec(spec)
                    try:
                        spec.loader.exec_module(mod)
                    except BaseException as e:
                        self._enrich_fn = None
                        self.errors.append(f"AI module import failed: {e}")
                        self.logger.warning(f"AI module import failed: {e}")
                    else:
                        self._enrich_fn = getattr(mod, "enrich_company", None)
                        if not callable(self._enrich_fn):
                            self._enrich_fn = None
                            self.errors.append("AI enrich_company not callable")
                            self.logger.warning("AI enrich_company not callable")
            else:
                self.logger.warning("AI enrichment module not found at req_scrapers/req_scrapers/ai_enhancment.py")
        except BaseException as e:
            self._enrich_fn = None
            self.logger.warning(f"Failed to load AI enrichment: {e}")
            self.errors.append(f"Failed to load AI enrichment: {e}")

    def close_spider(self, spider):
        # write a comprehensive run summary JSON
        try:
            self.end_time = datetime.utcnow()
            start_dt = self.start_time or self.end_time
            elapsed = self.end_time - start_dt
            elapsed_seconds = int(elapsed.total_seconds())
            elapsed_minutes = elapsed_seconds // 60
            remaining_seconds = elapsed_seconds % 60
            
            summary = {
                "run_id": self.run_id,
                "scraper_name": self.name,
                "source": self.source,
                "start_time_utc": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time_utc": self.end_time.strftime("%Y-%m-%d %H:%M:%S"),
                "total_duration": {
                    "seconds": elapsed_seconds,
                    "minutes": elapsed_minutes,
                    "remaining_seconds": remaining_seconds,
                    "formatted": f"{elapsed_minutes}m {remaining_seconds}s"
                },
                "statistics": {
                    "total_neqs_processed": len(self.neqs),
                    "total_companies_processed": self.stats["total_processed"],
                    "ai_enhanced_count": self.stats["ai_enhanced"],
                    "skipped_droit_circulation": self.stats["skipped_droit_circulation"],
                    "invalid_phone_skipped": self.stats["invalid_phone_skipped"],
                    "ai_limit_reached": self.stats["ai_limit_reached"],
                    "ai_count_limit": self.ai_count_limit,
                    "ai_enabled": self.use_ai
                },
                "errors": self.errors + self.stats["errors"],
                "error_count": len(self.errors) + len(self.stats["errors"]),
                "notes": (
                    f"Completed processing {self.stats['total_processed']} companies. "
                    f"AI enhanced: {self.stats['ai_enhanced']}, "
                    f"Skipped (droit_circulation): {self.stats['skipped_droit_circulation']}, "
                    f"Skipped (invalid_phone): {self.stats.get('invalid_phone_skipped', 0)}. "
                    f"AI limit reached: {self.stats['ai_limit_reached']}"
                )
            }
            
            # Resolve output path:
            # If user provided a path (absolute or relative), honor it; otherwise default to req_scrapers/output/
            provided = Path(self.summary_file)
            if provided.is_absolute() or provided.parent != Path(""):
                out_path = Path.cwd() / provided if not provided.is_absolute() else provided
                out_path.parent.mkdir(parents=True, exist_ok=True)
            else:
                out_dir = Path.cwd() / "req_scrapers" / "output"
                out_dir.mkdir(parents=True, exist_ok=True)
                out_path = out_dir / self.summary_file
            with out_path.open("w", encoding="utf-8") as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Run summary written to {out_path}")
            self.logger.info(f"Summary: {summary['notes']}")
        except Exception as e:
            self.logger.warning(f"Failed to write run summary: {e}")

    def start_requests(self):
        for neq in self.neqs:
            yield self.make_request(
                url=self.start_urls[0],
                callback=self.parse_initial,
                meta={"neq": neq, "cookiejar": f"jar-{neq}"},
            )

    def parse_initial(self, response):
        neq = response.meta["neq"]

        form_selector = response.xpath('//form[@id="mainForm"]')
        if not form_selector:
            try:
                self.errors.append("Missing main form on initial page")
            except Exception:
                pass
            return

        # Build formdata from existing inputs
        inputs = form_selector.xpath('.//input[@name]')
        formdata = {}
        for sel in inputs:
            name = sel.xpath('@name').get()
            value = sel.xpath('@value').get(default="")
            formdata[name] = value if value is not None else ""

        # Override with our search parameters
        formdata.update({
            "mainForm:typeDroit": formdata.get("mainForm:typeDroit", ""),
            "mainForm:personnePhysique": formdata.get("mainForm:personnePhysique", ""),
            "mainForm:municipalite": formdata.get("mainForm:municipalite", ""),
            "mainForm:municipaliteHorsQuebec": formdata.get("mainForm:municipaliteHorsQuebec", ""),
            "mainForm:neq": str(neq),
            "mainForm:nir": formdata.get("mainForm:nir", ""),
            "mainForm:ni": formdata.get("mainForm:ni", ""),
            "mainForm:ner": formdata.get("mainForm:ner", ""),
            "mainForm:nar": formdata.get("mainForm:nar", ""),
            "mainForm:numeroPermis": formdata.get("mainForm:numeroPermis", ""),
            "mainForm:numeroDemande": formdata.get("mainForm:numeroDemande", ""),
            "mainForm:numeroDossier": formdata.get("mainForm:numeroDossier", ""),
            "mainForm:j_id_32": "Rechercher",
            "mainForm_SUBMIT": formdata.get("mainForm_SUBMIT", "1"),
        })

        action = form_selector.xpath('./@action').get()
        if not action:
            try:
                self.errors.append("Missing form action on initial page")
            except Exception:
                pass
            return
        post_url = urljoin("https://www.pes.ctq.gouv.qc.ca", action)

        return self.make_request(
            url=post_url,
            callback=self.check_validity,
            meta={"neq": neq, "cookiejar": response.meta.get("cookiejar")},
            method="POST",
            formdata=formdata,
        )

    def check_validity(self, response):
        neq = response.meta["neq"]

        ctq_action = response.xpath('//form[@id="mainForm"]/@action').get()
        if ctq_action:
            ctq_final_url = urljoin("https://www.pes.ctq.gouv.qc.ca", ctq_action)

            if response.xpath('//h6[contains(text(),"Erreur(s)")]'):
                try:
                    self.errors.append(f"Invalid NEQ: {neq}")
                except Exception:
                    pass
                return  # Invalid NEQ

            match_text = response.xpath('//acronym/following-sibling::p/text()').get()
            if match_text == neq:
                yield self.make_request(
                    url=ctq_final_url,
                    callback=self.parse_ctq_result,
                    meta={"neq": neq, "cookiejar": response.meta.get("cookiejar")},
                    method="POST",
                    formdata=self.extract_form_data(response),
                )
        else:
            try:
                self.errors.append("Missing ctq action on validity page")
            except Exception:
                pass
            return

    def extract_form_data(self, response):
        # Prefer the specific PECVL link (registry details) for JSF target and params
        pecvl_onclick = response.xpath("//a[contains(@onclick, 'PECVL')]/@onclick").get()
        params = {}
        target_id = None
        if pecvl_onclick:
            try:
                parts = pecvl_onclick.split("submitForm(")[1].split(")")[0]
                # 'mainForm','mainForm:j_id_z_7_2',null,[[...]]
                pre, post = parts.split(",null,")
                target_id = pre.split(",")[1].strip().strip("'\"")
                params_str = post.strip()
                # strip surrounding [ and ] then split into key/value rows
                if params_str.startswith("[") and params_str.endswith("]"):
                    params_str = params_str[1:-1]
                rows = params_str.strip()[1:-1].split("],[")
                for row in rows:
                    key, value = row.replace("'", "").split(",")
                    params[key] = value
            except Exception:
                pass
        # Fallback: active tab's onclick/id
        if not params or not target_id:
            onclick_attr = response.xpath('//li[@class="classeOngletActif"]/a/@onclick').get()
            tab_click_id = response.xpath('//li[contains(@class, "classeOngletActif")]//a/@id').get()
            if onclick_attr:
                try:
                    parts = onclick_attr.split("[[")[1].split("]]")[0].split(",[")
                    for item in parts:
                        key, value = item.replace("'", "").split(",")
                        params[key] = value
                except Exception:
                    pass
            if not target_id:
                target_id = tab_click_id
        data = {
            "mainForm_SUBMIT": response.xpath('//form//input[@name="mainForm_SUBMIT"]/@value').get(),
            "javax.faces.ViewState": response.xpath('//form//input[@id="javax.faces.ViewState"]/@value').get(),
            # Prefer live values parsed from the page; fall back to conservative defaults
            "leClientNo": params.get("leClientNo", "129540"),
            "leContexte": params.get("leContexte", "PECVL"),
            "leOrderBy": params.get("leOrderBy", ""),
            "leOrderDir": params.get("leOrderDir", ""),
            "leContexteEstDejaDetermine": params.get("leContexteEstDejaDetermine", "oui"),
            "leDdrSeq": params.get("leDdrSeq", "0"),
            # Trigger the same JSF component as the site would
            "mainForm:_idcl": (target_id or "mainForm:j_id_z_7_2"),
        }
        return data

    def parse_ctq_result(self, response):
        neq = response.meta["neq"]
        table_xpath = '//table[contains(@class, "topTableauFixe")]'
        has_table = bool(response.xpath(table_xpath))
        if not has_table:
            try:
                self.errors.append(f"No result table for NEQ {neq}")
            except Exception:
                pass
            return

        def extract_text(xpath):
            return response.xpath(xpath).get(default="").strip()

        def normalize(p_list):
            return [p.strip() for p in p_list if p.strip()]

        # Adresse handling
        full_address_raw = response.xpath("//strong[normalize-space(.)=\"Adresse d'affaires\"]/following-sibling::p//text()").getall()
        full_address_lines = normalize(full_address_raw)

        adresse = full_address_lines[0] if len(full_address_lines) > 0 else ""
        ville, province, code_postal = "", "", ""

        if len(full_address_lines) > 1:
            parts = full_address_lines[1].split(" ")
            city_part = full_address_lines[1].split("(")[0].strip()
            province_part = full_address_lines[1].split("(")[1].split(")")[0].strip()
            postal_code_part = full_address_lines[1].split(")")[-1].strip()
            ville, province, code_postal = city_part, province_part, postal_code_part

        # NEQ extraction (shown in the top table), fallback to earlier check
        neq_text = response.xpath('//acronym[@title="Numéro d\'entreprise du Québec"]/following-sibling::p/text()').get()
        if neq_text:
            neq_text = neq_text.strip()

        # NIR extraction (may be on another tab; keep previous logic but tolerate missing)
        nir_list = response.xpath('(//acronym[@title="Numéro d\'identification au Registre"])[1]/following-sibling::p[1]/text()').getall()
        nir = ""
        for val in nir_list:
            val = val.strip()
            if val.startswith("R-") and val.count("-") >= 2:
                nir = val
                break

        # Build base record from CTQ (NEQ-side)
        base_company = {
            "nom": extract_text("//strong[normalize-space(.)='Nom']/following-sibling::p/text()"),
            "adresse": adresse,
            "ville": ville,
            "province": province,
            "code_postal": code_postal,
        }

        # Extract droit_circulation for filtering
        droit_circulation = extract_text("//strong[normalize-space(.)='Droit de mettre en circulation (Propriétaire)']/following-sibling::p/text()")
        
        # Update statistics
        self.stats["total_processed"] += 1
        
        # When AI is enabled, only process companies with droit_circulation == "Oui"
        # When AI is disabled, process all companies regardless of droit_circulation value
        if self.use_ai and droit_circulation.strip().lower() != "oui":
            self.stats["skipped_droit_circulation"] += 1
            self.logger.info(f"Skipping company with droit_circulation={droit_circulation} (AI enabled, only processing 'Oui'): {base_company['nom']}")
            return
        
        # Check if AI limit has been reached
        # Strictly enforce AI limit: stop enriching once limit reached
        if self.ai_count_limit and self.stats["ai_enhanced"] >= self.ai_count_limit:
            self.stats["ai_limit_reached"] = True
            self.logger.info(f"AI enhancement limit reached ({self.ai_count_limit}). Further AI calls will be skipped.")

        enriched = None
        should_use_ai = (
            self.use_ai and 
            self._enrich_fn and 
            not self.stats["ai_limit_reached"] and
            droit_circulation.strip().lower() == "oui"
        )
        
        if should_use_ai:
            try:
                self.logger.info(f"AI enhancing company: {base_company['nom']} (droit_circulation: {droit_circulation})")
                enriched = self._enrich_fn({
                    "nom": base_company["nom"],
                    "adresse": base_company["adresse"],
                    "ville": base_company["ville"],
                    "province": base_company["province"],
                    "code_postal": base_company["code_postal"],
                })
                # Increment only when we actually performed enrichment
                self.stats["ai_enhanced"] += 1
                self.logger.info(f"AI enhancement completed for: {base_company['nom']}")
            except BaseException as e:
                error_msg = f"AI enrichment failed for NEQ {(neq_text or neq)}: {e}"
                self.errors.append(error_msg)
                self.stats["errors"].append(error_msg)
                self.logger.debug(traceback.format_exc())
        # When AI is disabled, we process all companies (both "Oui" and "Non")
        # When AI is enabled, only "Oui" companies are processed and enhanced
        elif self.use_ai and not self._enrich_fn:
            error_msg = "AI enrichment unavailable; proceeding without AI data"
            self.errors.append(error_msg)
            self.stats["errors"].append(error_msg)

        # Map to standardized schema
        contacts = (enriched or {}).get("contacts") or []
        first_contact = contacts[0] if contacts else {}
        item = {
            "NEQ": (neq_text or neq) or "",
            "Employees": "",  # From REQ, not available here
            "other fields": json.dumps({
                "date_inscription": extract_text("//strong[normalize-space(.)=\"Date d'inscription au registre\"]/following-sibling::p/text()"),
                "date_prochaine_maj": extract_text("//strong[normalize-space(.)='Date limite de la prochaine mise à jour']/following-sibling::p/text()"),
                "code_securite": extract_text("//strong[normalize-space(.)='Cote de sécurité']/following-sibling::p/text()"),
                "droit_circulation": extract_text("//strong[normalize-space(.)='Droit de mettre en circulation (Propriétaire)']/following-sibling::p/text()"),
                "droit_exploiter": extract_text("//strong[normalize-space(.)=\"Droit d'exploiter (Exploitant)\"]/following-sibling::p/text()"),
                "motif": extract_text("//strong[normalize-space(.)='Motif']/following-sibling::p//text()[1]"),
            }, ensure_ascii=False),
            "NIR": nir,
            "company": base_company["nom"],
            "phone": (enriched or {}).get("phone_number", ""),
            "email": "",
            "website": (enriched or {}).get("company_website", ""),
            "address": base_company["adresse"],
            "city": base_company["ville"],
            "state": base_company["province"],
            "postal_code": base_company["code_postal"],
            "country": "CA",
            "phone_source": (enriched or {}).get("phone_number_source", ""),
            "reliability": (enriched or {}).get("reliability_level", ""),
            "first_name": first_contact.get("first_name", ""),
            "last_name": first_contact.get("last_name", ""),
            "title": first_contact.get("title", ""),
            "contact_source": first_contact.get("source", ""),
            "all_contacts": json.dumps(contacts, ensure_ascii=False),
            "note": (enriched or {}).get("notes", ""),
            "category": extract_text("//strong[normalize-space(.)='Catégorie de transport']/following-sibling::p/text()"),
            "source": self.source,
        }

        # Enforce E.164 phone format; skip items with invalid phone values when a phone is present
        def _is_valid_e164(phone: str) -> bool:
            if not phone:
                return False
            # E.164: + followed by 8 to 15 digits
            if not phone.startswith("+"):
                return False
            digits = phone[1:]
            return digits.isdigit() and 8 <= len(digits) <= 15

        phone_val = item.get("phone", "").strip()
        if phone_val and not _is_valid_e164(phone_val):
            self.stats["invalid_phone_skipped"] += 1
            self.logger.info(f"Skipping item due to invalid phone format (not E.164): {phone_val} | Company: {base_company['nom']}")
            return

        yield item

    # ---------------
    # Proxy utilities
    # ---------------
    def get_proxy_creds(self, index):
        if not self.proxy_list:
            return {"ip": "", "user": "", "pass": ""}
        entry = self.proxy_list[index % len(self.proxy_list)]
        parts = entry.split(":")
        if len(parts) == 4:
            ip, port, user, password = parts
            return {"ip": f"{ip}:{port}", "user": user, "pass": password}
        return {"ip": entry, "user": "", "pass": ""}

    def _next_proxy(self):
        if not self.proxy_list:
            return {"ip": "", "user": "", "pass": ""}
        self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
        return self.get_proxy_creds(self.current_proxy_index)

    def make_request(self, url, callback, meta=None, method="GET", formdata=None):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "close",
        }
        req_meta = {
            **(meta or {}),
            "dont_retry": True,
            "handle_httpstatus_list": [400, 403, 404, 429, 500, 502, 503, 504],
        }
        # Rotate proxy on every request
        if self.proxy_list:
            proxy = self._next_proxy()
            if proxy["user"] and proxy["pass"]:
                creds = f"{proxy['user']}:{proxy['pass']}"
                headers["Proxy-Authorization"] = "Basic " + base64.b64encode(creds.encode()).decode()
            else:
                try:
                    del headers["Proxy-Authorization"]
                except Exception:
                    pass
            req_meta["proxy"] = f"http://{proxy['ip']}"
        if method.upper() == "POST":
            return scrapy.FormRequest(
                url=url,
                method="POST",
                formdata=formdata or {},
                headers=headers,
                meta=req_meta,
                callback=callback,
                errback=self.handle_error,
                dont_filter=True,
            )
        else:
            return scrapy.Request(
                url=url,
                headers=headers,
                meta=req_meta,
                callback=callback,
                errback=self.handle_error,
                dont_filter=True,
            )

    def handle_error(self, failure):
        req = getattr(failure, "request", None)
        if not req:
            return
        if self.proxy_list:
            proxy = self._next_proxy()
            new_meta = dict(req.meta)
            new_meta["proxy"] = f"http://{proxy['ip']}"
            if proxy["user"] and proxy["pass"]:
                creds = f"{proxy['user']}:{proxy['pass']}"
                auth_value = "Basic " + base64.b64encode(creds.encode()).decode()
                req.headers["Proxy-Authorization"] = auth_value
            else:
                try:
                    del req.headers["Proxy-Authorization"]
                except Exception:
                    pass
            yield req.replace(meta=new_meta, dont_filter=True)

    def _load_neqs_from_file(self, file_path: str):
        values = []
        try:
            print(f"DEBUG: Attempting to load NEQs from: {file_path}")
            with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                print(f"DEBUG: CSV headers found: {reader.fieldnames}")
                # Accept common header variants for NEQ column
                target_headers = {"neq", "neq_numbers", "numéro d'entreprise du québec"}
                row_count = 0
                for row in reader:
                    row_count += 1
                    if not row:
                        continue
                    val = None
                    # Fast-path lookups for typical names
                    for key in ("NEQ", "neq", "neq_numbers"):
                        if key in row and (row.get(key) or "").strip():
                            val = row.get(key)
                            break
                    # Fallback: scan headers case-insensitively
                    if val is None:
                        for k, v in row.items():
                            if not k:
                                continue
                            norm = k.lower().strip()
                            if norm in target_headers:
                                val = v
                                break
                    if val is not None:
                        s = str(val).strip()
                        if s:
                            values.append(s)
                print(f"DEBUG: Processed {row_count} rows, found {len(values)} valid NEQs")
        except Exception as e:
            print(f"DEBUG: Error loading NEQs from file: {e}")
            pass
        return values
