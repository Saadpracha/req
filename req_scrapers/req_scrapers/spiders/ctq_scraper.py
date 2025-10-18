import csv
import json
import base64
import scrapy
from scrapy.http import FormRequest
from urllib.parse import urljoin
from datetime import datetime
from pathlib import Path


class CtqScraperSpider(scrapy.Spider):
    name = "ctq_scraper"
    allowed_domains = ["pes.ctq.gouv.qc.ca"]
    start_urls = ["https://www.pes.ctq.gouv.qc.ca/pes2/mvc/dossierclient"]
    custom_settings = {
        "LOG_LEVEL": "DEBUG",
    }

    # Proxy state
    proxy_list = []
    current_proxy_index = 0
    total_requests = 0
    errors = 0

    def __init__(self, neqs=None, file=None, start_neq=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.neqs = neqs.split(",") if neqs else []
        if file:
            self.neqs.extend(self._load_neqs_from_file(file))
        self.neqs = list(dict.fromkeys([str(n).strip() for n in self.neqs if str(n).strip()]))

        if start_neq and start_neq in self.neqs:
            start_index = self.neqs.index(start_neq)
            self.neqs = self.neqs[start_index:]
            self.logger.info(f"Resuming from NEQ {start_neq} (index {start_index})")
        elif start_neq:
            self.logger.warning(f"Start NEQ {start_neq} not found. Starting from beginning.")

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
                            # Log first few proxies for debugging (without credentials)
                            for i, proxy in enumerate(self.proxy_list[:3]):
                                parts = proxy.split(":")
                                if len(parts) >= 2:
                                    self.logger.debug(f"Proxy {i+1}: {parts[0]}:{parts[1]}")
                                else:
                                    self.logger.debug(f"Proxy {i+1}: {proxy}")
                            if len(self.proxy_list) > 3:
                                self.logger.debug(f"... and {len(self.proxy_list) - 3} more proxies")
                    else:
                        self.logger.warning("proxies.json content is not a list; skipping proxies load")
            else:
                self.logger.info("proxies.json not found; running without proxies")
        except Exception as e:
            self.logger.warning(f"Failed to load proxies.json: {e}")

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
            return

        inputs = form_selector.xpath('.//input[@name]')
        formdata = {sel.xpath('@name').get(): sel.xpath('@value').get(default="") for sel in inputs}
        formdata.update({
            "mainForm:neq": str(neq),
            "mainForm:j_id_32": "Rechercher",
            "mainForm_SUBMIT": formdata.get("mainForm_SUBMIT", "1"),
        })

        action = form_selector.xpath('./@action').get()
        if not action:
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
        if not ctq_action:
            return

        if response.xpath('//h6[contains(text(),"Erreur(s)")]'):
            return  # Invalid NEQ

        match_text = response.xpath('//acronym/following-sibling::p/text()').get()
        if match_text == neq:
            # Check for VRAC onclick before proceeding with CTQ data
            vrac_onclick = response.xpath('(//a[contains(text(),"Registre du camionnage en vrac")])[1]/@onclick').get()
            self.logger.debug(f"VRAC onclick: {vrac_onclick}")
            
            if vrac_onclick:
                # If VRAC link found, make POST request to get VRAC data first, then CTQ data
                vrac_payload = {
                    "leClientNo": "4007",
                    "leContexte": "VRAC",
                    "leOrderBy": "",
                    "leOrderDir": "",
                    "leContexteEstDejaDetermine": "oui",
                    "leDdrSeq": "0",
                    "mainForm:_idcl": "mainForm:j_id_z_8_2",
                    "mainForm_SUBMIT": response.xpath('//form//input[@name="mainForm_SUBMIT"]/@value').get(),
                    "javax.faces.ViewState": response.xpath('//form//input[@id="javax.faces.ViewState"]/@value').get(),
                }
                
                return self.make_request(
                    url=response.url,
                    callback=self.parse_vrac_result,
                    meta={"neq": neq, "cookiejar": response.meta.get("cookiejar"), "has_vrac": True, "ctq_action": ctq_action, "ctq_formdata": self.extract_form_data(response)},
                    method="POST",
                    formdata=vrac_payload,
                )
            else:
                # No VRAC link found, proceed with normal CTQ data extraction
                return self.make_request(
                    url=urljoin("https://www.pes.ctq.gouv.qc.ca", ctq_action),
                    callback=self.parse_ctq_result,
                    meta={"neq": neq, "cookiejar": response.meta.get("cookiejar"), "has_vrac": False},
                    method="POST",
                    formdata=self.extract_form_data(response),
                )

    def extract_form_data(self, response):
        onclick = response.xpath("//a[contains(@onclick, 'PECVL')]/@onclick").get()
        params = {}
        target_id = None

        if onclick:
            try:
                parts = onclick.split("submitForm(")[1].split(")")[0]
                pre, post = parts.split(",null,")
                target_id = pre.split(",")[1].strip().strip("'\"")
                rows = post.strip()[2:-2].split("],[")
                for row in rows:
                    key, value = row.replace("'", "").split(",")
                    params[key] = value
            except Exception:
                pass

        return {
            "mainForm_SUBMIT": response.xpath('//form//input[@name="mainForm_SUBMIT"]/@value').get(),
            "javax.faces.ViewState": response.xpath('//form//input[@id="javax.faces.ViewState"]/@value').get(),
            "leClientNo": params.get("leClientNo", "129540"),
            "leContexte": params.get("leContexte", "PECVL"),
            "leOrderBy": params.get("leOrderBy", ""),
            "leOrderDir": params.get("leOrderDir", ""),
            "leContexteEstDejaDetermine": params.get("leContexteEstDejaDetermine", "oui"),
            "leDdrSeq": params.get("leDdrSeq", "0"),
            "mainForm:_idcl": target_id or "mainForm:j_id_z_7_2",
        }

    def parse_ctq_result(self, response):
        neq = response.meta["neq"]

        def extract_text(xpath):
            return response.xpath(xpath).get(default="").strip()

        def normalize(lines):
            return [line.strip() for line in lines if line.strip()]

        def format_date(value: str):
            if not value:
                return ""
            value = value.strip()
            
            # Try different date formats and return with time if original had time
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M"):
                try:
                    parsed_date = datetime.strptime(value, fmt)
                    return parsed_date.strftime("%Y-%m-%d %H:%M")
                except ValueError:
                    continue
            
            # Try date-only formats
            for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d", "%d %B %Y", "%d %b %Y"):
                try:
                    parsed_date = datetime.strptime(value, fmt)
                    return parsed_date.strftime("%Y-%m-%d")
                except ValueError:
                    continue
            
            # Try ISO format
            try:
                parsed_date = datetime.fromisoformat(value)
                return parsed_date.strftime("%Y-%m-%d %H:%M")
            except Exception:
                pass
            
            # If all else fails, return original value
            return value

        full_address_lines = normalize(
            response.xpath("//strong[normalize-space(.)=\"Adresse d'affaires\"]/following-sibling::p//text()").getall()
        )
        adresse = full_address_lines[0] if full_address_lines else ""
        ville = province = code_postal = ""

        if len(full_address_lines) > 1:
            try:
                parts = full_address_lines[1].split("(")
                ville = parts[0].strip()
                province = parts[1].split(")")[0].strip()
                code_postal = parts[1].split(")")[1].strip()
            except Exception:
                pass

        base_item = {
            "neq": extract_text('//acronym[@title="Numéro d\'entreprise du Québec"]/following-sibling::p/text()') or neq,
            "nom": extract_text("//strong[normalize-space(.)='Nom']/following-sibling::p/text()"),
            "full_address": " ".join(full_address_lines),
            "adresse": adresse,
            "ville": ville,
            "province": province,
            "code_postal": code_postal,
            "nir": next((val.strip() for val in response.xpath('(//acronym[@title="Numéro d\'identification au Registre"])[1]/following-sibling::p[1]/text()').getall() if val.strip().startswith("R-")), ""),
            "titre": extract_text("//strong[normalize-space(.)='Titre']/following-sibling::p/text()"),
            "categorie_transport": extract_text("//strong[normalize-space(.)='Catégorie de transport']/following-sibling::p/text()"),
            "date_inscription": format_date(extract_text("//strong[normalize-space(.)=\"Date d'inscription au registre\"]/following-sibling::p/text()")),
            "date_prochaine_maj": format_date(extract_text("//strong[normalize-space(.)='Date limite de la prochaine mise à jour']/following-sibling::p/text()")),
            "code_securite": extract_text("//strong[normalize-space(.)='Cote de sécurité']/following-sibling::p/text()"),
            "droit_circulation": extract_text("//strong[normalize-space(.)='Droit de mettre en circulation (Propriétaire)']/following-sibling::p/text()"),
            "droit_exploiter": extract_text("//strong[normalize-space(.)=\"Droit d'exploiter (Exploitant)\"]/following-sibling::p/text()"),
            "motif": extract_text("//strong[normalize-space(.)='Motif']/following-sibling::p//text()[1]"),
            # Default VRAC fields (empty when no VRAC data)
            "vrac_numero_inscription": "",
            "vrac_region_exploitation": "",
            "vrac_nombre_camions": "",
            "vrac_nom_courtier": "",
        }

        yield base_item
    def extract_onclick_formdata(self, onclick_str, response):
        try:
            if "submitForm(" not in onclick_str:
                return {}, None
            parts = onclick_str.split("submitForm(")[1].split(")")[0]
            pre, post = parts.split(",null,")
            target_id = pre.split(",")[1].strip().strip("'\"")
            rows = post.strip()[2:-2].split("],[")
            params = {}
            for row in rows:
                key, value = row.replace("'", "").split(",")
                params[key] = value
            return params, target_id
        except Exception as e:
            self.logger.warning(f"Failed to extract onclick form data: {e}")
            return {}, None

    def parse_vrac_result(self, response):
        neq = response.meta["neq"]
        ctq_action = response.meta["ctq_action"]
        ctq_formdata = response.meta["ctq_formdata"]
        
        # First extract VRAC data
        def extract_text(xpath):
            return response.xpath(xpath).get(default="").strip()

        # Extract VRAC data from the correct table structure
        # Based on debug output: Cell 1,0: '7-C-505522', Cell 1,1: '', Cell 1,2: '2', Cell 1,3: 'VRAC-RICHELIEU'
        vrac_data = {
            "vrac_numero_inscription": extract_text("(//table[@class='tableContenu']//tr[td]/td)[1]/text()"),
            "vrac_region_exploitation": extract_text("(//table[@class='tableContenu']//tr[td]/td)[2]//a/text()"),
            "vrac_nombre_camions": extract_text("(//table[@class='tableContenu']//tr[td]/td)[3]/text()"),
            "vrac_nom_courtier": extract_text("(//table[@class='tableContenu']//tr[td]/td)[4]/text()"),
        }
        
        self.logger.debug(f"VRAC data extracted: {vrac_data}")
        
        # Now get CTQ data using the stored form data
        if ctq_action:
            self.logger.debug(f"Making CTQ request with action: {ctq_action}")
            yield self.make_request(
                url=urljoin("https://www.pes.ctq.gouv.qc.ca", ctq_action),
                callback=self.parse_ctq_result_with_vrac,
                meta={"neq": neq, "cookiejar": response.meta.get("cookiejar"), "vrac_data": vrac_data},
                method="POST",
                formdata=ctq_formdata,
            )
        else:
            # If no CTQ action, create a minimal item with VRAC data
            base_item = {
                "neq": neq,
                "nom": "",
                "full_address": "",
                "adresse": "",
                "ville": "",
                "province": "",
                "code_postal": "",
                "nir": "",
                "titre": "",
                "categorie_transport": "",
                "date_inscription": "",
                "date_prochaine_maj": "",
                "code_securite": "",
                "droit_circulation": "",
                "droit_exploiter": "",
                "motif": "",
            }
            base_item.update(vrac_data)
            yield base_item

    def parse_ctq_result_with_vrac(self, response):
        neq = response.meta["neq"]
        vrac_data = response.meta["vrac_data"]

        def extract_text(xpath):
            return response.xpath(xpath).get(default="").strip()

        def normalize(lines):
            return [line.strip() for line in lines if line.strip()]

        def format_date(value: str):
            if not value:
                return ""
            value = value.strip()
            
            # Try different date formats and return with time if original had time
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M"):
                try:
                    parsed_date = datetime.strptime(value, fmt)
                    return parsed_date.strftime("%Y-%m-%d %H:%M")
                except ValueError:
                    continue
            
            # Try date-only formats
            for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d", "%d %B %Y", "%d %b %Y"):
                try:
                    parsed_date = datetime.strptime(value, fmt)
                    return parsed_date.strftime("%Y-%m-%d")
                except ValueError:
                    continue
            
            # Try ISO format
            try:
                parsed_date = datetime.fromisoformat(value)
                return parsed_date.strftime("%Y-%m-%d %H:%M")
            except Exception:
                pass
            
            # If all else fails, return original value
            return value

        full_address_lines = normalize(
            response.xpath("//strong[normalize-space(.)=\"Adresse d'affaires\"]/following-sibling::p//text()").getall()
        )
        adresse = full_address_lines[0] if full_address_lines else ""
        ville = province = code_postal = ""

        if len(full_address_lines) > 1:
            try:
                parts = full_address_lines[1].split("(")
                ville = parts[0].strip()
                province = parts[1].split(")")[0].strip()
                code_postal = parts[1].split(")")[1].strip()
            except Exception:
                pass

        base_item = {
            "neq": extract_text('//acronym[@title="Numéro d\'entreprise du Québec"]/following-sibling::p/text()') or neq,
            "nom": extract_text("//strong[normalize-space(.)='Nom']/following-sibling::p/text()"),
            "full_address": " ".join(full_address_lines),
            "adresse": adresse,
            "ville": ville,
            "province": province,
            "code_postal": code_postal,
            "nir": next((val.strip() for val in response.xpath('(//acronym[@title="Numéro d\'identification au Registre"])[1]/following-sibling::p[1]/text()').getall() if val.strip().startswith("R-")), ""),
            "titre": extract_text("//strong[normalize-space(.)='Titre']/following-sibling::p/text()"),
            "categorie_transport": extract_text("//strong[normalize-space(.)='Catégorie de transport']/following-sibling::p/text()"),
            "date_inscription": format_date(extract_text("//strong[normalize-space(.)=\"Date d'inscription au registre\"]/following-sibling::p/text()")),
            "date_prochaine_maj": format_date(extract_text("//strong[normalize-space(.)='Date limite de la prochaine mise à jour']/following-sibling::p/text()")),
            "code_securite": extract_text("//strong[normalize-space(.)='Cote de sécurité']/following-sibling::p/text()"),
            "droit_circulation": extract_text("//strong[normalize-space(.)='Droit de mettre en circulation (Propriétaire)']/following-sibling::p/text()"),
            "droit_exploiter": extract_text("//strong[normalize-space(.)=\"Droit d'exploiter (Exploitant)\"]/following-sibling::p/text()"),
            "motif": extract_text("//strong[normalize-space(.)='Motif']/following-sibling::p//text()[1]"),
        }
        
        # Add VRAC data to the base item
        base_item.update(vrac_data)
        self.logger.debug(f"Final combined item: {base_item}")
        yield base_item

    def _load_neqs_from_file(self, file_path: str):
        values = []
        try:
            with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    val = row.get("NEQ") or next((v for k, v in row.items() if k.lower().strip() == "neq"), None)
                    if val and str(val).strip():
                        values.append(str(val).strip())
        except Exception as e:
            self.logger.warning(f"Error loading NEQs from file: {e}")
        return values

    # ------------------------
    # Proxy helpers and request builder
    # ------------------------

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
        proxy = self.get_proxy_creds(self.current_proxy_index)
        self.logger.debug(f"Rotated to proxy index {self.current_proxy_index}: {proxy['ip']}")
        return proxy

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
                self.logger.debug(f"Using authenticated proxy: {proxy['ip']} (user: {proxy['user']})")
            else:
                try:
                    del headers["Proxy-Authorization"]
                except Exception:
                    pass
                self.logger.debug(f"Using proxy: {proxy['ip']}")
            req_meta["proxy"] = f"http://{proxy['ip']}"
        else:
            self.logger.debug("No proxies available - using direct connection")

        self.total_requests += 1
        
        # Log request details
        neq = meta.get("neq", "unknown") if meta else "unknown"
        self.logger.debug(f"Request #{self.total_requests} for NEQ {neq}: {method} {url}")

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
            self.logger.error("Failure missing request: %s", failure)
            self.errors += 1
            return

        if self.proxy_list:
            proxy = self._next_proxy()

            # Update meta with new proxy
            new_meta = dict(req.meta)
            new_meta["proxy"] = f"http://{proxy['ip']}"

            # Update headers with new Proxy-Authorization if needed
            if proxy["user"] and proxy["pass"]:
                creds = f"{proxy['user']}:{proxy['pass']}"
                auth_value = "Basic " + base64.b64encode(creds.encode()).decode()
                req.headers["Proxy-Authorization"] = auth_value
                self.logger.warning(
                    "Request failed: %s — rotating to authenticated proxy: %s (user: %s) and retrying",
                    req.url, proxy['ip'], proxy['user']
                )
            else:
                try:
                    del req.headers["Proxy-Authorization"]
                except Exception:
                    pass
                self.logger.warning(
                    "Request failed: %s — rotating to proxy: %s and retrying",
                    req.url, proxy['ip']
                )

            yield req.replace(meta=new_meta, dont_filter=True)
        else:
            self.logger.error("Request failed and no proxies available: %s", req.url)
            self.errors += 1
