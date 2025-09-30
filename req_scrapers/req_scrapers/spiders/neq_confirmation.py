import scrapy
from urllib.parse import urljoin, urlparse
from pathlib import Path
import csv
import json
import base64
import os


class ReqScraperSpider(scrapy.Spider):
    name = "req_scraper"
    allowed_domains = ["pes.ctq.gouv.qc.ca", "pes.rbq.gouv.qc.ca"]

    # List of NEQs to check
    neq_list = []
    proxy_list = []
    current_proxy_index = 0
    total_requests = 0
    errors = 0
    use_residential_proxy = False
    resi_proxy_host = ""
    resi_proxy_port = ""
    resi_proxy_user = ""
    resi_proxy_pass = ""

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        # Create spider via base implementation to ensure crawler is set
        spider = super(ReqScraperSpider, cls).from_crawler(crawler, *args, **kwargs)

        # Load residential proxy settings from Scrapy settings or environment
        settings = crawler.settings
        spider.resi_proxy_host = str(settings.get("RESI_PROXY_HOST", os.getenv("RESI_PROXY_HOST", ""))).strip()
        spider.resi_proxy_port = str(settings.get("RESI_PROXY_PORT", os.getenv("RESI_PROXY_PORT", ""))).strip()
        spider.resi_proxy_user = str(settings.get("RESI_PROXY_USER", os.getenv("RESI_PROXY_USER", ""))).strip()
        spider.resi_proxy_pass = str(settings.get("RESI_PROXY_PASS", os.getenv("RESI_PROXY_PASS", ""))).strip()

        if spider.resi_proxy_host and spider.resi_proxy_port and spider.resi_proxy_user and spider.resi_proxy_pass:
            spider.use_residential_proxy = True
            spider.logger.info(
                "Using residential proxy endpoint %s:%s (country configured in credentials)",
                spider.resi_proxy_host,
                spider.resi_proxy_port,
            )
        return spider

    def __init__(self, input_file: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Require explicit input file; no default. Column expected: "Neq_numbers"

        # Load proxies from proxies.json at project root if present (fallback when no residential proxy)
        try:
            proxies_path = Path.cwd() / "proxies.json"
            if proxies_path.is_file():
                # Use utf-8-sig to tolerate BOM
                with proxies_path.open("r", encoding="utf-8-sig") as pf:
                    loaded = json.load(pf)
                    if isinstance(loaded, list):
                        # Expect entries like "ip:port:user:pass" or "ip:port"
                        self.proxy_list = [str(x).strip() for x in loaded if str(x).strip()]
                        if self.proxy_list:
                            self.logger.info(f"Loaded {len(self.proxy_list)} proxies from {proxies_path}")
                    else:
                        self.logger.warning("proxies.json content is not a list; skipping proxies load")
            else:
                self.logger.info("proxies.json not found; running without proxies")
        except Exception as e:
            self.logger.warning(f"Failed to load proxies.json: {e}")

        self.neq_list = []
        if not input_file:
            self.logger.warning("No input_file provided. Use -a input_file=path/to/file.csv")
        else:
            file_path = Path(input_file)
            if not file_path.is_file():
                self.logger.warning(f"NEQ input file not found: {file_path}. No NEQs will be processed.")
            else:
                neq_numbers: list[str] = []
                try:
                    with file_path.open("r", newline="", encoding="utf-8-sig") as f:
                        reader = csv.DictReader(f)
                        if reader.fieldnames is None:
                            self.logger.warning(f"CSV has no header: {file_path}")
                        # Determine NEQ column flexibly
                        target_col = None
                        fieldnames = reader.fieldnames or []
                        normalized = {col: col.strip().lower() for col in fieldnames}
                        accepted = {"neq_numbers", "neq", "neques", "req", "neqnumber", "neqs"}

                        # Exact accepted names
                        for original, norm in normalized.items():
                            if norm in accepted:
                                target_col = original
                                break

                        # Contains 'neq'
                        if target_col is None:
                            for original, norm in normalized.items():
                                if "neq" in norm:
                                    target_col = original
                                    break

                        # Single-column CSV fallback
                        if target_col is None and len(fieldnames) == 1:
                            target_col = fieldnames[0]

                        if target_col is None:
                            self.logger.warning("Could not find NEQ column. Expected one of: Neq_numbers, NEQ, neq. Please set header accordingly.")
                        else:
                            self.logger.info(f"Using NEQ column: {target_col}")
                            for row in reader:
                                raw_val = (row.get(target_col) or "").strip()
                                if raw_val:
                                    neq_numbers.append(raw_val)
                except Exception as e:
                    self.logger.error(f"Failed to read NEQ file {file_path}: {e}")

                if neq_numbers:
                    self.neq_list = neq_numbers
                    self.logger.info(f"Loaded {len(self.neq_list)} NEQ numbers from {file_path}")
                else:
                    self.logger.warning("No NEQ numbers loaded. The spider will not produce results.")

    def start_requests(self):
        for neq in self.neq_list:
            # Start CTQ check for each NEQ
            yield self.make_request(
                url="https://www.pes.ctq.gouv.qc.ca/pes2/mvc/dossierclient",
                callback=self.init_ctq_request,
                meta={"neq": neq}
            )

    def init_ctq_request(self, response):
        neq = response.meta["neq"]
        ctq_payload = {
            "mainForm:typeDroit": "",
            "mainForm:personnePhysique": "",
            "mainForm:municipalite": "",
            "mainForm:municipaliteHorsQuebec": "",
            "mainForm:neq": neq,
            "mainForm:nir": "",
            "mainForm:ni": "",
            "mainForm:ner": "",
            "mainForm:nar": "",
            "mainForm:numeroPermis": "",
            "mainForm:numeroDemande": "",
            "mainForm:numeroDossier": "",
            "mainForm:j_id_32": "Rechercher",
            "mainForm_SUBMIT": "1",
            "javax.faces.ViewState": "e1s1",
        }

        yield self.make_request(
            url=response.url,
            callback=self.parse_ctq_redirect,
            meta={"neq": neq},
            method="POST",
            formdata=ctq_payload
        )

    def parse_ctq_redirect(self, response):
        neq = response.meta["neq"]
        ctq_action = response.xpath('//form[@id="mainForm"]/@action').get()

        if ctq_action:
            ctq_final_url = urljoin("https://www.pes.ctq.gouv.qc.ca", ctq_action)
            yield self.make_request(
                url=ctq_final_url,
                callback=self.parse_ctq_result,
                meta={"neq": neq}
            )
        else:
            # Assume not found and continue to RBQ
            yield from self.start_rbq_check(neq, ctq_result="No")

    def parse_ctq_result(self, response):
        neq = response.meta["neq"]

        if response.xpath('//h6[contains(text(),"Erreur(s)")]'):
            ctq_result = "No"
        elif response.xpath('//acronym/following-sibling::p/text()').get() == neq:
            ctq_result = "Yes"
        else:
            ctq_result = "No"

        yield from self.start_rbq_check(neq, ctq_result)

    def start_rbq_check(self, neq, ctq_result):
        rbq_url = "https://www.pes.rbq.gouv.qc.ca/RegistreLicences/Recherche?mode=Entreprise"
        yield self.make_request(
            url=rbq_url,
            callback=self.init_rbq_request,
            meta={"neq": neq, "ctq_result": ctq_result}
        )

    def init_rbq_request(self, response):
        neq = response.meta["neq"]
        ctq_result = response.meta["ctq_result"]

        rbq_payload = {
            "NomEntreprise": "",
            "NoLicence": "",
            "NEQ": neq,
            "NoTelephone": "",
            "g-recaptcha-response": ""
        }

        yield self.make_request(
            url=response.url,
            callback=self.parse_rbq_redirect,
            meta={"neq": neq, "ctq_result": ctq_result},
            method="POST",
            formdata=rbq_payload
        )

    def parse_rbq_redirect(self, response):
        neq = response.meta["neq"]
        ctq_result = response.meta["ctq_result"]

        rbq_action = response.xpath('//form/@action').get()
        if rbq_action:
            rbq_final_url = urljoin("https://www.pes.rbq.gouv.qc.ca", rbq_action)
            yield self.make_request(
                url=rbq_final_url,
                callback=self.parse_rbq_result,
                meta={"neq": neq, "ctq_result": ctq_result}
            )
        else:
            # No RBQ results page. Only yield if CTQ is Yes.
            if ctq_result == "Yes":
                yield {
                    "NEQ": neq,
                    "RBQ": "No",
                    "CTQ": ctq_result
                }

    def parse_rbq_result(self, response):
        neq = response.meta["neq"]
        ctq_result = response.meta["ctq_result"]

        if response.xpath('(//nav[@aria-label="Page de résultats :"])[1]'):
            rbq_result = "Yes"
        else:
            rbq_result = "No"

        # Only yield if at least one is Yes
        if not (rbq_result == "No" and ctq_result == "No"):
            yield {
                "NEQ": neq,
                "RBQ": rbq_result,
                "CTQ": ctq_result
            }

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

    def make_request(self, url, callback, meta=None, method="GET", formdata=None):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            # Force new TCP connection for each request to encourage a new IP from the pool
            "Connection": "close",
        }

        req_meta = {
            **(meta or {}),
            "dont_retry": True,
            "handle_httpstatus_list": [400, 403, 404, 429, 500, 502, 503, 504],
        }

        if self.use_residential_proxy:
            creds = f"{self.resi_proxy_user}:{self.resi_proxy_pass}"
            headers["Proxy-Authorization"] = "Basic " + base64.b64encode(creds.encode()).decode()
            req_meta["proxy"] = f"http://{self.resi_proxy_host}:{self.resi_proxy_port}"
        elif self.proxy_list:
            proxy = self.get_proxy_creds(self.current_proxy_index)
            if proxy["user"] and proxy["pass"]:
                creds = f"{proxy['user']}:{proxy['pass']}"
                headers["Proxy-Authorization"] = "Basic " + base64.b64encode(creds.encode()).decode()
            req_meta["proxy"] = f"http://{proxy['ip']}"

        self.total_requests += 1

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

        if self.use_residential_proxy:
            # Retry with the same endpoint; closing connection should get a different IP
            new_meta = dict(req.meta)
            new_meta["proxy"] = f"http://{self.resi_proxy_host}:{self.resi_proxy_port}"
            creds = f"{self.resi_proxy_user}:{self.resi_proxy_pass}"
            auth_value = "Basic " + base64.b64encode(creds.encode()).decode()
            req.headers["Proxy-Authorization"] = auth_value
            req.headers["Connection"] = "close"

            self.logger.warning(
                "Request failed: %s — retrying via residential endpoint (new IP expected)",
                req.url,
            )
            yield req.replace(meta=new_meta, dont_filter=True)
        elif self.proxy_list:
            prev = self.current_proxy_index
            self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
            proxy = self.get_proxy_creds(self.current_proxy_index)

            # Update meta with new proxy
            new_meta = dict(req.meta)
            new_meta["proxy"] = f"http://{proxy['ip']}"

            # Update headers with new Proxy-Authorization if needed
            if proxy["user"] and proxy["pass"]:
                creds = f"{proxy['user']}:{proxy['pass']}"
                auth_value = "Basic " + base64.b64encode(creds.encode()).decode()
                req.headers["Proxy-Authorization"] = auth_value
            else:
                # Remove header if present
                try:
                    del req.headers["Proxy-Authorization"]
                except Exception:
                    pass

            self.logger.warning(
                "Request failed: %s — rotating proxy %d -> %d and retrying",
                req.url, prev, self.current_proxy_index
            )

            yield req.replace(meta=new_meta, dont_filter=True)
        else:
            self.logger.error("Request failed and no proxies available: %s", req.url)
            self.errors += 1
