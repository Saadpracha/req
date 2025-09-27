import scrapy
from urllib.parse import urljoin, urlparse
from pathlib import Path
import csv
import json
import base64
import random
import time


class ReqScraperSpider(scrapy.Spider):
    name = "req_scraper"
    allowed_domains = ["pes.ctq.gouv.qc.ca", "pes.rbq.gouv.qc.ca"]

    # List of NEQs to check
    neq_list = []
    total_requests = 0
    errors = 0
    processed_neqs = set()  # Track processed NEQs to avoid duplicates

    def __init__(self, input_file: str = None, randomize: str = "true", *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Require explicit input file; no default. Column expected: "Neq_numbers"
        
        self.randomize_neqs = randomize.lower() == "true"
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
                    if self.randomize_neqs:
                        random.shuffle(neq_numbers)
                        self.logger.info(f"Randomized NEQ order for {len(neq_numbers)} NEQs")
                    self.neq_list = neq_numbers
                    self.logger.info(f"Loaded {len(self.neq_list)} NEQ numbers from {file_path}")
                else:
                    self.logger.warning("No NEQ numbers loaded. The spider will not produce results.")

    def start_requests(self):
        for i, neq in enumerate(self.neq_list):
            # Add random delay between requests to avoid detection
            if i > 0:
                delay = random.uniform(2, 5)  # Random delay between 2-5 seconds
                time.sleep(delay)
            
            # Track processed NEQs
            if neq in self.processed_neqs:
                self.logger.warning(f"NEQ {neq} already processed, skipping")
                continue
            
            self.processed_neqs.add(neq)
            
            # Skip the initial GET request and go directly to POST
            # This avoids redundant requests to the same URL
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
            
            # Go directly to POST request - no initial GET needed
            yield self.make_request(
                url="https://www.pes.ctq.gouv.qc.ca/pes2/mvc/dossierclient",
                callback=self.parse_ctq_redirect,
                meta={"neq": neq, "skip_retry": True},  # Skip retries for this request
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
                meta={"neq": neq, "skip_retry": True}
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
            meta={"neq": neq, "ctq_result": ctq_result, "skip_retry": True}
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
            meta={"neq": neq, "ctq_result": ctq_result, "skip_retry": True},
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
                meta={"neq": neq, "ctq_result": ctq_result, "skip_retry": True}
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
    # Request builder
    # ------------------------

    def make_request(self, url, callback, meta=None, method="GET", formdata=None):
        # All anti-detection logic is now handled by ComprehensiveAntiDetectionMiddleware
        req_meta = {
            **(meta or {}),
            "dont_retry": meta.get("skip_retry", False),  # Skip retries if requested
            "handle_httpstatus_list": [400, 403, 404, 429, 500, 502, 503, 504],
        }

        self.total_requests += 1

        if method.upper() == "POST":
            return scrapy.FormRequest(
                url=url,
                method="POST",
                formdata=formdata or {},
                meta=req_meta,
                callback=callback,
                errback=self.handle_error,
                dont_filter=True,
            )
        else:
            return scrapy.Request(
                url=url,
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

        self.logger.error("Request failed: %s - %s", req.url, failure.value)
        self.errors += 1

    def closed(self, reason):
        """Called when the spider is closed"""
        self.logger.info(f"Spider closed: {reason}")
        self.logger.info(f"Total requests made: {self.total_requests}")
        self.logger.info(f"Total errors: {self.errors}")
        self.logger.info(f"NEQs processed: {len(self.processed_neqs)}")
        if self.total_requests > 0:
            error_rate = (self.errors / self.total_requests) * 100
            self.logger.info(f"Error rate: {error_rate:.2f}%")
