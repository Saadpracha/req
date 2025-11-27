import csv
import base64
import re
import scrapy
from scrapy.http import FormRequest
from urllib.parse import urljoin
from datetime import datetime
from pathlib import Path
import json


class CtqScraperSpider(scrapy.Spider):
    name = "ctq_scraper"
    allowed_domains = ["pes.ctq.gouv.qc.ca"]
    start_urls = ["https://www.pes.ctq.gouv.qc.ca/pes2/mvc/dossierclient"]
    custom_settings = {  # Randomize delay by 0.5 seconds
        "USER_AGENT": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
        ),
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
                      "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Upgrade-Insecure-Requests": "1",
        },
    }

    # Proxy state
    proxy_list = []
    current_proxy_index = 0
    total_requests = 0
    errors = 0

    def __init__(self, neqs=None, file=None, start_neq=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Print startup message to confirm debugging is enabled
        print("\n" + "="*80, flush=True)
        print("DEBUGGING MODE ENABLED - Full request/response logging active", flush=True)
        print("="*80 + "\n", flush=True)
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
                            print(f"{'='*80}", flush=True)
                            print(f"PROXY CONFIGURATION: Loaded {len(self.proxy_list)} proxies from {proxies_path}", flush=True)
                            self.logger.info(f"{'='*80}")
                            self.logger.info(f"PROXY CONFIGURATION: Loaded {len(self.proxy_list)} proxies from {proxies_path}")
                            # Log all proxies (masking passwords for security)
                            for i, proxy_entry in enumerate(self.proxy_list):
                                parts = proxy_entry.split(":")
                                if len(parts) >= 4:
                                    # Format: IP:PORT:USER:PASS
                                    ip_port = f"{parts[0]}:{parts[1]}"
                                    user = parts[2]
                                    msg = f"  Proxy #{i+1}: {ip_port} (user: {user}, password: ***)"
                                    print(msg, flush=True)
                                    self.logger.info(msg)
                                elif len(parts) >= 2:
                                    # Format: IP:PORT
                                    msg = f"  Proxy #{i+1}: {parts[0]}:{parts[1]} (no auth)"
                                    print(msg, flush=True)
                                    self.logger.info(msg)
                                else:
                                    msg = f"  Proxy #{i+1}: {proxy_entry}"
                                    print(msg, flush=True)
                                    self.logger.info(msg)
                            msg = f"Proxy rotation will cycle through all {len(self.proxy_list)} proxies"
                            print(msg, flush=True)
                            print(f"{'='*80}", flush=True)
                            self.logger.info(msg)
                            self.logger.info(f"{'='*80}")
                    else:
                        self.logger.warning("proxies.json content is not a list; skipping proxies load")
            else:
                self.logger.info("proxies.json not found; running without proxies")
        except Exception as e:
            self.logger.warning(f"Failed to load proxies.json: {e}")

    def start_requests(self):
        for neq in self.neqs:
            self.logger.debug(f"Starting request for NEQ {neq} to {self.start_urls[0]}")
            yield from self.make_request(
                url=self.start_urls[0],
                callback=self.parse_initial,
                meta={"neq": neq, "cookiejar": f"jar-{neq}"},
            )

    def parse_initial(self, response):
        neq = response.meta["neq"]
        
        # Enhanced debugging for initial response
        proxy_used = response.meta.get("proxy", "none")
        self.logger.debug(f"parse_initial: NEQ {neq}, status={response.status}, proxy={proxy_used}, body_size={len(response.body) if response.body else 0}")
        
        if response.status != 200:
            self.logger.warning(f"Non-200 status code {response.status} in parse_initial for NEQ {neq} with proxy {proxy_used}")
            if response.body:
                self.logger.debug(f"Response body preview (first 500 chars): {response.body[:500]}")

        form_selector = response.xpath('//form[@id="mainForm"]')
        if not form_selector:
            self.logger.warning(f"Form not found in parse_initial for NEQ {neq} with proxy {proxy_used}")
            return

        view_state = response.xpath('//input[@name="javax.faces.ViewState"]/@value').get()
        if not view_state:
            return

        first_request_payload = {
            "mainForm:typeDroit": "",
            "mainForm:personnePhysique": "",
            "mainForm:municipalite": "",
            "mainForm:municipaliteHorsQuebec": "",
            "mainForm:neq": str(neq),
            "mainForm:nir": "",
            "mainForm:ni": "",
            "mainForm:ner": "",
            "mainForm:nar": "",
            "mainForm:numeroPermis": "",
            "mainForm:numeroDemande": "",
            "mainForm:numeroDossier": "",
            "mainForm:j_id_32": "Rechercher",
            "mainForm_SUBMIT": "1",
            "javax.faces.ViewState": view_state,
        }

        headers = {
            "Origin": "https://www.pes.ctq.gouv.qc.ca",
            "Referer": response.url,
            "Content-Type": "application/x-www-form-urlencoded",
        }
        
        req_meta = {
            "neq": neq,
            "cookiejar": response.meta.get("cookiejar"),
            "dont_retry": True,
            "handle_httpstatus_list": [400, 401, 403, 404, 429, 500, 502, 503, 504],
        }
        
        if self.proxy_list:
            proxy = self._next_proxy()
            proxy_url = f"http://{proxy['ip']}"
            if proxy["user"] and proxy["pass"]:
                creds = f"{proxy['user']}:{proxy['pass']}"
                headers["Proxy-Authorization"] = "Basic " + base64.b64encode(creds.encode()).decode()
                self.logger.debug(f"Using proxy {proxy_url} with auth (user: {proxy['user']}) for {response.url}")
            else:
                self.logger.debug(f"Using proxy {proxy_url} without auth for {response.url}")
            req_meta["proxy"] = proxy_url
            self.logger.debug(f"Proxy URL set in meta: {req_meta.get('proxy')}")
        else:
            self.logger.debug(f"Making request without proxy for {response.url}")

        self.logger.debug(f"Making POST request to {response.url} with status handling: {req_meta.get('handle_httpstatus_list')}")
        
        yield scrapy.FormRequest(
            url=response.url,
            formdata=first_request_payload,
            headers=headers,
            method="POST",
            callback=self.check_validity,
            errback=self.handle_error,
            meta=req_meta,
            dont_filter=True,
        )

    def check_validity(self, response):
        neq = response.meta["neq"]
        
        # Enhanced debugging for response
        proxy_used = response.meta.get("proxy", "none")
        self.logger.debug(f"Response received for NEQ {neq}: status={response.status}, proxy={proxy_used}, body_size={len(response.body) if response.body else 0}")
        
        if response.status != 200:
            self.logger.warning(f"Non-200 status code {response.status} for NEQ {neq} with proxy {proxy_used}")
            if response.body:
                self.logger.debug(f"Response body preview (first 500 chars): {response.body[:500]}")
        
        if response.status == 401 or not response.body:
            self.logger.warning(f"Request failed for NEQ {neq}: status={response.status}, body_empty={not response.body}, proxy={proxy_used}")
            return
        
        view_state = response.xpath('//input[@name="javax.faces.ViewState"]/@value').get()
        redirect_execution = view_state if view_state else "e1s2"
        
        ctq_action = response.xpath('//form[@id="mainForm"]/@action').get()
        if not ctq_action:
            return

        if response.xpath('//h6[contains(text(),"Erreur(s)")]'):
            return

        match_text = response.xpath('//acronym/following-sibling::p/text()').get()
        if match_text == neq:
            extra_values_list = response.xpath('(//div[@class="client"])[1]/ul/li/a/text()').getall()
            extra_values = ",".join([val.strip() for val in extra_values_list if val.strip()])
            
            vrac_onclick = response.xpath('(//a[contains(text(),"Registre du camionnage en vrac")])[1]/@onclick').get()
            
            if vrac_onclick:
                vrac_second_request_payload = {
                    "mainForm_SUBMIT": "1",
                    "javax.faces.ViewState": redirect_execution,
                    "leClientNo": "4007",
                    "leContexte": "VRAC",
                    "leOrderBy": "",
                    "leOrderDir": "",
                    "leContexteEstDejaDetermine": "oui",
                    "leDdrSeq": "0",
                    "mainForm:_idcl": "mainForm:j_id_z_8_2",
                }
                
                second_request_url = response.url
                if "execution=" not in second_request_url:
                    separator = "&" if "?" in second_request_url else "?"
                    second_request_url = f"{second_request_url}{separator}execution={redirect_execution}"
                else:
                    second_request_url = re.sub(r'execution=[^&;]*', f'execution={redirect_execution}', second_request_url)
                
                headers = {
                    "Origin": "https://www.pes.ctq.gouv.qc.ca",
                    "Referer": response.url,
                    "Content-Type": "application/x-www-form-urlencoded",
                }
                
                req_meta = {
                    "neq": neq,
                    "cookiejar": response.meta.get("cookiejar"),
                    "has_vrac": True,
                    "ctq_action": ctq_action,
                    "ctq_formdata": self.extract_form_data(response, redirect_execution),
                    "extra_values": extra_values,
                    "dont_retry": True,
                    "handle_httpstatus_list": [400, 401, 403, 404, 429, 500, 502, 503, 504],
                }
                
                if self.proxy_list:
                    proxy = self._next_proxy()
                    proxy_url = f"http://{proxy['ip']}"
                    if proxy["user"] and proxy["pass"]:
                        creds = f"{proxy['user']}:{proxy['pass']}"
                        headers["Proxy-Authorization"] = "Basic " + base64.b64encode(creds.encode()).decode()
                        self.logger.debug(f"Using proxy {proxy_url} with auth for VRAC request to {second_request_url}")
                    else:
                        self.logger.debug(f"Using proxy {proxy_url} without auth for VRAC request to {second_request_url}")
                    req_meta["proxy"] = proxy_url
                
                self.logger.debug(f"Making VRAC POST request to {second_request_url} for NEQ {neq}")
                
                yield scrapy.FormRequest(
                    url=second_request_url,
                    formdata=vrac_second_request_payload,
                    headers=headers,
                    method="POST",
                    callback=self.parse_vrac_result,
                    errback=self.handle_error,
                    meta=req_meta,
                    dont_filter=True,
                )
            else:
                ctq_second_request_payload = self.extract_form_data(response, redirect_execution)
                
                second_request_url = urljoin("https://www.pes.ctq.gouv.qc.ca", ctq_action)
                if "execution=" not in second_request_url:
                    separator = "&" if "?" in second_request_url else "?"
                    second_request_url = f"{second_request_url}{separator}execution={redirect_execution}"
                else:
                    second_request_url = re.sub(r'execution=[^&;]*', f'execution={redirect_execution}', second_request_url)
                
                headers = {
                    "Origin": "https://www.pes.ctq.gouv.qc.ca",
                    "Referer": response.url,
                    "Content-Type": "application/x-www-form-urlencoded",
                }
                
                req_meta = {
                    "neq": neq,
                    "cookiejar": response.meta.get("cookiejar"),
                    "has_vrac": False,
                    "extra_values": extra_values,
                    "dont_retry": True,
                    "handle_httpstatus_list": [400, 401, 403, 404, 429, 500, 502, 503, 504],
                }
                
                if self.proxy_list:
                    proxy = self._next_proxy()
                    proxy_url = f"http://{proxy['ip']}"
                    if proxy["user"] and proxy["pass"]:
                        creds = f"{proxy['user']}:{proxy['pass']}"
                        headers["Proxy-Authorization"] = "Basic " + base64.b64encode(creds.encode()).decode()
                        self.logger.debug(f"Using proxy {proxy_url} with auth for CTQ request to {second_request_url}")
                    else:
                        self.logger.debug(f"Using proxy {proxy_url} without auth for CTQ request to {second_request_url}")
                    req_meta["proxy"] = proxy_url
                
                self.logger.debug(f"Making CTQ POST request to {second_request_url} for NEQ {neq}")
                
                yield scrapy.FormRequest(
                    url=second_request_url,
                    formdata=ctq_second_request_payload,
                    headers=headers,
                    method="POST",
                    callback=self.parse_ctq_result,
                    errback=self.handle_error,
                    meta=req_meta,
                    dont_filter=True,
                )

    def extract_form_data(self, response, execution=None):
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

        # Use execution parameter if provided, otherwise extract from response
        view_state = execution
        if not view_state:
            view_state = response.xpath('//form//input[@id="javax.faces.ViewState"]/@value').get()
        if not view_state:
            view_state = "1"  # Default fallback

        # Build a completely separate payload for the second request
        # This is a strict, individual payload - not updating the same variable
        second_request_payload = {
            "mainForm_SUBMIT": "1",
            "javax.faces.ViewState": view_state,
            "leClientNo": params.get("leClientNo", "129540"),
            "leContexte": params.get("leContexte", "PECVL"),
            "leOrderBy": params.get("leOrderBy", ""),
            "leOrderDir": params.get("leOrderDir", ""),
            "leContexteEstDejaDetermine": params.get("leContexteEstDejaDetermine", "oui"),
            "leDdrSeq": params.get("leDdrSeq", "0"),
            "mainForm:_idcl": target_id or "mainForm:j_id_z_7_2",
        }
        
        return second_request_payload

    def parse_ctq_result(self, response):
        neq = response.meta["neq"]
        
        # Enhanced debugging for response
        proxy_used = response.meta.get("proxy", "none")
        self.logger.debug(f"parse_ctq_result: NEQ {neq}, status={response.status}, proxy={proxy_used}, body_size={len(response.body) if response.body else 0}")
        
        if response.status != 200:
            self.logger.warning(f"Non-200 status code {response.status} in parse_ctq_result for NEQ {neq} with proxy {proxy_used}")

        def extract_text(xpath):
            return response.xpath(xpath).get(default="").strip()

        def normalize(lines):
            return [line.strip() for line in lines if line.strip()]

        def add_delimiter(value: str):
            """Return value as-is (CSV exporter will handle quoting)"""
            if not value:
                return ""
            return value

        def extract_pays_from_address(address_lines):
            """Extract text before postal code and add to pays column"""
            pays = ""
            if len(address_lines) > 2:
                # Look for text before postal code in the last line
                last_line = address_lines[-1]
                # Extract postal code pattern (letters/numbers followed by postal code)
                postal_pattern = r'[A-Za-z0-9\s]+(\d{5}|\d{3}\s?\d{3}|\d{2}\s?\d{3})'
                match = re.search(postal_pattern, last_line)
                if match:
                    # Get text before the postal code
                    postal_start = match.start()
                    text_before_postal = last_line[:postal_start].strip()
                    if text_before_postal:
                        pays = text_before_postal
            return pays
        
        full_address_lines = normalize(
            response.xpath("//strong[normalize-space(.)=\"Adresse d'affaires\"]/following-sibling::p//text()").getall()
        )
        adresse = full_address_lines[0] if full_address_lines else ""
        ville = province = code_postal = ""
        pays = extract_pays_from_address(full_address_lines)

        if len(full_address_lines) > 1:
            try:
                parts = full_address_lines[1].split("(")
                ville = parts[0].strip()
                province = parts[1].split(")")[0].strip()
                code_postal = parts[1].split(")")[1].strip()
            except Exception:
                pass
        
        # Extract extra values from meta (passed from check_validity)
        extra_values = response.meta.get("extra_values", "")

        date_inscription = response.xpath(
            "(//strong[contains(text(),\"Date d'inscription au registre\")]/following-sibling::p/text())[1]"
        ).get(default="").strip()
        date_prochaine_maj = response.xpath(
            "(//strong[normalize-space(.)='Date limite de la prochaine mise à jour']/following-sibling::p/text())[1]"
        ).get(default="").strip()

        base_item = {
            "neq": extract_text('//acronym[@title="Numéro d\'entreprise du Québec"]/following-sibling::p/text()') or neq,
            "nom": add_delimiter(extract_text("//strong[normalize-space(.)='Nom']/following-sibling::p/text()")),
            "full_address": add_delimiter(" ".join(full_address_lines)),
            "adresse": add_delimiter(adresse),
            "ville": add_delimiter(ville),
            "province": add_delimiter(province),
            "code_postal": add_delimiter(code_postal),
            "pays": add_delimiter(pays),
            "nir": add_delimiter(next((val.strip() for val in response.xpath('(//acronym[@title="Numéro d\'identification au Registre"])[1]/following-sibling::p[1]/text()').getall() if val.strip().startswith("R-")), "")),
            "titre": add_delimiter(extract_text("//strong[normalize-space(.)='Titre']/following-sibling::p/text()")),
            "categorie_transport": add_delimiter(extract_text("//strong[normalize-space(.)='Catégorie de transport']/following-sibling::p/text()")),
            "date_inscription": self._format_excel_text(date_inscription),
            "date_prochaine_maj": self._format_excel_text(date_prochaine_maj),
            "code_securite": add_delimiter(extract_text("//strong[normalize-space(.)='Cote de sécurité']/following-sibling::p/text()")),
            "droit_circulation": add_delimiter(extract_text("//strong[normalize-space(.)='Droit de mettre en circulation (Propriétaire)']/following-sibling::p/text()")),
            "droit_exploiter": add_delimiter(extract_text("//strong[normalize-space(.)=\"Droit d'exploiter (Exploitant)\"]/following-sibling::p/text()")),
            "motif": add_delimiter(extract_text("//strong[normalize-space(.)='Motif']/following-sibling::p//text()[1]")),
            "extra_values": add_delimiter(extra_values),
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
        except Exception:
            return {}, None

    def parse_vrac_result(self, response):
        neq = response.meta["neq"]
        ctq_action = response.meta["ctq_action"]
        ctq_formdata = response.meta["ctq_formdata"]
        
        # Enhanced debugging for response
        proxy_used = response.meta.get("proxy", "none")
        self.logger.debug(f"parse_vrac_result: NEQ {neq}, status={response.status}, proxy={proxy_used}, body_size={len(response.body) if response.body else 0}")
        
        if response.status != 200:
            self.logger.warning(f"Non-200 status code {response.status} in parse_vrac_result for NEQ {neq} with proxy {proxy_used}")
        
        def extract_text(xpath):
            return response.xpath(xpath).get(default="").strip()
        
        def add_delimiter(value: str):
            """Return value as-is (CSV exporter will handle quoting)"""
            if not value:
                return ""
            return value


        vrac_data = {
            "vrac_numero_inscription": add_delimiter(extract_text("(//table[@class='tableContenu']//tr[td]/td)[1]/text()")),
            "vrac_region_exploitation": add_delimiter(extract_text("(//table[@class='tableContenu']//tr[td]/td)[2]//a/text()")),
            "vrac_nombre_camions": add_delimiter(extract_text("(//table[@class='tableContenu']//tr[td]/td)[3]/text()")),
            "vrac_nom_courtier": add_delimiter(extract_text("(//table[@class='tableContenu']//tr[td]/td)[4]/text()")),
        }
        
        if ctq_action:
            ctq_url = urljoin("https://www.pes.ctq.gouv.qc.ca", ctq_action)
            
            headers = {
                "Origin": "https://www.pes.ctq.gouv.qc.ca",
                "Referer": response.url,
                "Content-Type": "application/x-www-form-urlencoded",
            }
            
            req_meta = {
                "neq": neq,
                "cookiejar": response.meta.get("cookiejar"),
                "vrac_data": vrac_data,
                "extra_values": response.meta.get("extra_values", ""),
                "dont_retry": True,
                "handle_httpstatus_list": [400, 401, 403, 404, 429, 500, 502, 503, 504],
            }
            
            if self.proxy_list:
                proxy = self._next_proxy()
                proxy_url = f"http://{proxy['ip']}"
                if proxy["user"] and proxy["pass"]:
                    creds = f"{proxy['user']}:{proxy['pass']}"
                    headers["Proxy-Authorization"] = "Basic " + base64.b64encode(creds.encode()).decode()
                    self.logger.debug(f"Using proxy {proxy_url} with auth for CTQ-with-VRAC request to {ctq_url}")
                else:
                    self.logger.debug(f"Using proxy {proxy_url} without auth for CTQ-with-VRAC request to {ctq_url}")
                req_meta["proxy"] = proxy_url
            
            self.logger.debug(f"Making CTQ-with-VRAC POST request to {ctq_url} for NEQ {neq}")
            
            yield scrapy.FormRequest(
                url=ctq_url,
                formdata=ctq_formdata,
                headers=headers,
                method="POST",
                callback=self.parse_ctq_result_with_vrac,
                errback=self.handle_error,
                meta=req_meta,
                dont_filter=True,
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
                "pays": "",
                "nir": "",
                "titre": "",
                "categorie_transport": "",
                "date_inscription": "",
                "date_prochaine_maj": "",
                "code_securite": "",
                "droit_circulation": "",
                "droit_exploiter": "",
                "motif": "",
                "extra_values": "",
            }
            base_item.update(vrac_data)
            yield base_item

    def parse_ctq_result_with_vrac(self, response):
        neq = response.meta["neq"]
        vrac_data = response.meta["vrac_data"]
        
        # Enhanced debugging for response
        proxy_used = response.meta.get("proxy", "none")
        self.logger.debug(f"parse_ctq_result_with_vrac: NEQ {neq}, status={response.status}, proxy={proxy_used}, body_size={len(response.body) if response.body else 0}")
        
        if response.status != 200:
            self.logger.warning(f"Non-200 status code {response.status} in parse_ctq_result_with_vrac for NEQ {neq} with proxy {proxy_used}")

        def extract_text(xpath):
            return response.xpath(xpath).get(default="").strip()

        def normalize(lines):
            return [line.strip() for line in lines if line.strip()]

        def add_delimiter(value: str):
            """Return value as-is (CSV exporter will handle quoting)"""
            if not value:
                return ""
            return value
        
        def extract_pays_from_address(address_lines):
            """Extract text before postal code and add to pays column"""
            pays = ""
            if len(address_lines) > 2:
                # Look for text before postal code in the last line
                last_line = address_lines[-1]
                # Extract postal code pattern (letters/numbers followed by postal code)
                postal_pattern = r'[A-Za-z0-9\s]+(\d{5}|\d{3}\s?\d{3}|\d{2}\s?\d{3})'
                match = re.search(postal_pattern, last_line)
                if match:
                    # Get text before the postal code
                    postal_start = match.start()
                    text_before_postal = last_line[:postal_start].strip()
                    if text_before_postal:
                        pays = text_before_postal
            return pays
        
        full_address_lines = normalize(
            response.xpath("//strong[normalize-space(.)=\"Adresse d'affaires\"]/following-sibling::p//text()").getall()
        )
        adresse = full_address_lines[0] if full_address_lines else ""
        ville = province = code_postal = ""
        pays = extract_pays_from_address(full_address_lines)

        if len(full_address_lines) > 1:
            try:
                parts = full_address_lines[1].split("(")
                ville = parts[0].strip()
                province = parts[1].split(")")[0].strip()
                code_postal = parts[1].split(")")[1].strip()
            except Exception:
                pass
        
        # Extract extra values from meta (passed from check_validity)
        extra_values = response.meta.get("extra_values", "")

        date_inscription = response.xpath(
            "(//strong[contains(text(),\"Date d'inscription au registre\")]/following-sibling::p/text())[1]"
        ).get(default="").strip()
        date_prochaine_maj = response.xpath(
            "(//strong[normalize-space(.)='Date limite de la prochaine mise à jour']/following-sibling::p/text())[1]"
        ).get(default="").strip()

        base_item = {
            "neq": extract_text('//acronym[@title="Numéro d\'entreprise du Québec"]/following-sibling::p/text()') or neq,
            "nom": add_delimiter(extract_text("//strong[normalize-space(.)='Nom']/following-sibling::p/text()")),
            "full_address": add_delimiter(" ".join(full_address_lines)),
            "adresse": add_delimiter(adresse),
            "ville": add_delimiter(ville),
            "province": add_delimiter(province),
            "code_postal": add_delimiter(code_postal),
            "pays": add_delimiter(pays),
            "nir": add_delimiter(next((val.strip() for val in response.xpath('(//acronym[@title="Numéro d\'identification au Registre"])[1]/following-sibling::p[1]/text()').getall() if val.strip().startswith("R-")), "")),
            "titre": add_delimiter(extract_text("//strong[normalize-space(.)='Titre']/following-sibling::p/text()")),
            "categorie_transport": add_delimiter(extract_text("//strong[normalize-space(.)='Catégorie de transport']/following-sibling::p/text()")),
            "date_inscription": self._format_excel_text(date_inscription),
            "date_prochaine_maj": self._format_excel_text(date_prochaine_maj),
            "code_securite": add_delimiter(extract_text("//strong[normalize-space(.)='Cote de sécurité']/following-sibling::p/text()")),
            "droit_circulation": add_delimiter(extract_text("//strong[normalize-space(.)='Droit de mettre en circulation (Propriétaire)']/following-sibling::p/text()")),
            "droit_exploiter": add_delimiter(extract_text("//strong[normalize-space(.)=\"Droit d'exploiter (Exploitant)\"]/following-sibling::p/text()")),
            "motif": add_delimiter(extract_text("//strong[normalize-space(.)='Motif']/following-sibling::p//text()[1]")),
            "extra_values": add_delimiter(extra_values),
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
        except Exception:
            pass
        return values

    # ------------------------
    # Proxy helpers and request builder
    # ------------------------

    def get_proxy_creds(self, index):
        if not self.proxy_list:
            return {"ip": "", "user": "", "pass": ""}

        entry = self.proxy_list[index % len(self.proxy_list)]
        self.logger.debug(f"get_proxy_creds: Raw entry from list: {entry}")
        
        # Try colon separator first (format: IP:PORT:USER:PASS)
        parts = entry.split(":")
        self.logger.debug(f"get_proxy_creds: Split by colon - parts: {parts}, count: {len(parts)}")
        
        # If colon split didn't work, try comma separator (format: IP,PORT,USER,PASS)
        if len(parts) != 4:
            parts = entry.split(",")
            self.logger.debug(f"get_proxy_creds: Split by comma - parts: {parts}, count: {len(parts)}")
        
        if len(parts) == 4:
            ip, port, user, password = parts
            result = {"ip": f"{ip}:{port}", "user": user, "pass": password}
            self.logger.debug(f"get_proxy_creds: Parsed proxy - ip:port={result['ip']}, user={result['user']}")
            return result
        
        # Fallback: try to parse as IP:PORT (no auth) with either separator
        if len(parts) == 2:
            ip, port = parts
            result = {"ip": f"{ip}:{port}", "user": "", "pass": ""}
            self.logger.debug(f"get_proxy_creds: Parsed as IP:PORT (no auth) - {result['ip']}")
            return result
        
        # Last resort: use entry as-is
        result = {"ip": entry, "user": "", "pass": ""}
        self.logger.warning(f"get_proxy_creds: Could not parse proxy entry, using as-is: {result['ip']}")
        return result

    def _next_proxy(self):
        if not self.proxy_list:
            return {"ip": "", "user": "", "pass": ""}
        self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
        self.logger.debug(f"_next_proxy: Rotating to index {self.current_proxy_index} (total proxies: {len(self.proxy_list)})")
        result = self.get_proxy_creds(self.current_proxy_index)
        self.logger.debug(f"_next_proxy: Returning proxy - ip:port={result['ip']}, has_auth={bool(result['user'] and result['pass'])}")
        return result

    @staticmethod
    def _format_excel_text(value: str):
        """Prefix value to prevent Excel from auto-formatting"""
        if not value:
            return ""
        return f"{value}"

    def make_request(self, url, callback, meta=None, method="GET", formdata=None):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,fr;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Cache-Control": "max-age=0",
        }
        
        if meta and meta.get("referer"):
            headers["Referer"] = meta.get("referer")

        req_meta = {
            **(meta or {}),
            "dont_retry": True,
            "handle_httpstatus_list": [400, 401, 403, 404, 429, 500, 502, 503, 504],
        }

        if self.proxy_list:
            proxy = self._next_proxy()
            proxy_url = f"http://{proxy['ip']}"
            if proxy["user"] and proxy["pass"]:
                creds = f"{proxy['user']}:{proxy['pass']}"
                headers["Proxy-Authorization"] = "Basic " + base64.b64encode(creds.encode()).decode()
                self.logger.debug(f"Using proxy {proxy_url} with auth (user: {proxy['user']}) for {url}")
            else:
                try:
                    del headers["Proxy-Authorization"]
                except Exception:
                    pass
                self.logger.debug(f"Using proxy {proxy_url} without auth for {url}")
            req_meta["proxy"] = proxy_url
            self.logger.debug(f"Proxy URL set in meta: {req_meta.get('proxy')}")
        else:
            self.logger.debug(f"Making request without proxy for {url}")

        self.total_requests += 1
        
        if method.upper() == "POST":
            yield scrapy.FormRequest(
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
            yield scrapy.Request(
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
            self.errors += 1
            self.logger.error(f"Error handler called without request object. Failure type: {type(failure)}")
            return

        # Enhanced error logging
        neq = req.meta.get("neq", "unknown")
        proxy_used = req.meta.get("proxy", "none")
        url = req.url if req else "unknown"
        
        # Log the failure details
        failure_type = type(failure.value).__name__ if hasattr(failure, 'value') and failure.value else type(failure).__name__
        failure_msg = str(failure.value) if hasattr(failure, 'value') and failure.value else str(failure)
        
        self.logger.error(f"Request failed for NEQ {neq} to {url}: {failure_type} - {failure_msg}")
        self.logger.error(f"Proxy used: {proxy_used}")
        
        # Log response if available
        if hasattr(failure, 'response') and failure.response:
            self.logger.error(f"Response status: {failure.response.status}, body_size: {len(failure.response.body) if failure.response.body else 0}")
            if failure.response.body:
                self.logger.debug(f"Response body preview (first 500 chars): {failure.response.body[:500]}")

        if self.proxy_list:
            proxy = self._next_proxy()
            proxy_url = f"http://{proxy['ip']}"
            new_meta = dict(req.meta)
            new_meta["proxy"] = proxy_url
            
            self.logger.debug(f"Retrying with new proxy {proxy_url} for NEQ {neq}")

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
        else:
            self.errors += 1
            self.logger.error(f"No proxies available for retry. Marking as error for NEQ {neq}")
