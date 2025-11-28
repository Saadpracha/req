import csv
import base64
import re
import scrapy
from scrapy.http import FormRequest
from urllib.parse import urljoin
from datetime import datetime
from pathlib import Path
import json
import random


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
        "DOWNLOADER_CLIENT_CONTEXTFACTORY": "scrapy.core.downloader.contextfactory.AcceptAnyServerTlsContextFactory",
        "DOWNLOADER_CLIENT_TLS_METHOD": "TLS",
    }

    # Proxy state
    proxy_list = []
    current_proxy_index = 0
    total_requests = 0
    errors = 0
    
    # User agents for rotation
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    ]
    
    # Accept-Language variations
    accept_languages = [
        "en-US,en;q=0.9",
        "en-US,en;q=0.9,fr;q=0.8",
        "en-CA,en;q=0.9,fr;q=0.8",
        "fr-CA,fr;q=0.9,en;q=0.8",
        "en-US,en;q=0.9,fr-CA;q=0.8",
    ]

    def __init__(self, neqs=None, file=None, start_neq=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Print startup message to confirm debugging is enabled
        print("\n" + "="*80, flush=True)
        print("DEBUGGING MODE ENABLED - Full request/response logging active", flush=True)
        print("="*80 + "\n", flush=True)
        inline_neqs = [str(n).strip() for n in neqs.split(",")] if neqs else []
        inline_neqs = [n for n in inline_neqs if n]

        start_neq_clean = str(start_neq).strip() if start_neq else None
        start_exists = False
        if start_neq_clean:
            if start_neq_clean in inline_neqs:
                start_exists = True
            elif file and self._value_exists_in_file(file, start_neq_clean):
                start_exists = True
            if start_exists:
                self.logger.info(f"Resuming from NEQ {start_neq_clean}")
            else:
                self.logger.warning(f"Start NEQ {start_neq_clean} not found. Starting from beginning.")
                start_neq_clean = None

        # Create generator for NEQ values (memory-efficient for large files)
        self.neqs = self._iter_neq_values(inline_neqs, file, start_neq_clean)
        
        # Log initialization
        inline_count = len(inline_neqs)
        file_info = f" + file: {file}" if file else ""
        self.logger.info(f"Initialized NEQ generator: {inline_count} inline values{file_info}")

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
                            # Test parsing to verify format
                            for i, proxy_entry in enumerate(self.proxy_list):
                                # Try both formats for logging
                                if "," in proxy_entry:
                                    parts = proxy_entry.split(",")
                                    format_type = "comma-separated"
                                else:
                                    parts = proxy_entry.split(":")
                                    format_type = "colon-separated"
                                
                                if len(parts) >= 4:
                                    # Format: IP:PORT:USER:PASS or IP,PORT,USER,PASS
                                    ip_port = f"{parts[0]}:{parts[1]}"
                                    user = parts[2]
                                    msg = f"  Proxy #{i+1}: {ip_port} (user: {user}, password: ***) [{format_type}]"
                                    print(msg, flush=True)
                                    self.logger.info(msg)
                                elif len(parts) >= 2:
                                    # Format: IP:PORT or IP,PORT
                                    msg = f"  Proxy #{i+1}: {parts[0]}:{parts[1]} (no auth) [{format_type}]"
                                    print(msg, flush=True)
                                    self.logger.info(msg)
                                else:
                                    msg = f"  Proxy #{i+1}: {proxy_entry} [UNPARSED - may cause issues]"
                                    print(msg, flush=True)
                                    self.logger.warning(msg)
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
            proxy_info = "none"
            if self.proxy_list:
                # Preview which proxy will be used (next one in rotation)
                proxy_preview = self._get_current_proxy()
                proxy_info = proxy_preview["ip"].split(":")[0] if proxy_preview["ip"] else "none"
            self.logger.debug(f"Starting initial GET | NEQ={neq} | Proxy={proxy_info}")
            yield from self.make_request(
                url=self.start_urls[0],
                callback=self.parse_initial,
                meta={"neq": neq, "cookiejar": f"jar-{neq}"},
            )

    def parse_initial(self, response):
        neq = response.meta["neq"]
        
        # Enhanced debugging for initial response with prominent status code
        proxy_used = response.meta.get("proxy", "none")
        status = response.status
        body_size = len(response.body) if response.body else 0
        
        # Extract proxy IP for cleaner logging
        proxy_ip = "none"
        if proxy_used != "none" and proxy_used:
            try:
                proxy_ip = proxy_used.split("://")[1].split(":")[0] if "://" in proxy_used else proxy_used.split(":")[0]
            except:
                proxy_ip = proxy_used
        
        self.logger.info(f"parse_initial: status={status} | NEQ={neq} | Proxy={proxy_ip} | BodySize={body_size}")
        
        if status != 200:
            self.logger.warning(f"parse_initial non-200 status {status} for NEQ {neq}")
            return

        form_selector = response.xpath('//form[@id="mainForm"]')
        if not form_selector:
            self.logger.warning(f"Form not found for NEQ {neq}")
            return

        view_state = response.xpath('//input[@name="javax.faces.ViewState"]/@value').get()
        if not view_state:
            self.logger.warning(f"ViewState not found in parse_initial for NEQ {neq}")
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

        headers = self._get_random_headers(referer=response.url, include_user_agent=True)
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        
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
        
        self.logger.debug(f"Using User-Agent: {headers.get('User-Agent', 'default')[:50]}...")

        proxy_ip = req_meta.get("proxy", "none").split("://")[1].split(":")[0] if req_meta.get("proxy", "none") != "none" and "://" in req_meta.get("proxy", "") else "none"
        self.logger.debug(f"Making POST to check_validity | NEQ={neq} | Proxy={proxy_ip}")
        self.logger.debug(f"POST target {response.url} with status handling: {req_meta.get('handle_httpstatus_list')}")
        
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
        
        # Enhanced debugging for response with prominent status code
        proxy_used = response.meta.get("proxy", "none")
        status = response.status
        body_size = len(response.body) if response.body else 0
        
        # Extract proxy IP for cleaner logging
        proxy_ip = "none"
        if proxy_used != "none" and proxy_used:
            try:
                proxy_ip = proxy_used.split("://")[1].split(":")[0] if "://" in proxy_used else proxy_used.split(":")[0]
            except:
                proxy_ip = proxy_used
        
        self.logger.info(f"check_validity: status={status} | NEQ={neq} | Proxy={proxy_ip} | BodySize={body_size}")
        
        if status != 200:
            self.logger.warning(f"check_validity non-200 status {status} for NEQ {neq}")
            return
        
        if not response.body:
            self.logger.warning(f"check_validity empty body for NEQ {neq}")
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
                
                headers = self._get_random_headers(referer=response.url, include_user_agent=True)
                headers["Content-Type"] = "application/x-www-form-urlencoded"
                
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
                
                self.logger.debug(f"Using User-Agent: {headers.get('User-Agent', 'default')[:50]}...")
                
                proxy_ip = req_meta.get("proxy", "none").split("://")[1].split(":")[0] if req_meta.get("proxy", "none") != "none" and "://" in req_meta.get("proxy", "") else "none"
                self.logger.debug(f"Making POST to parse_vrac_result | NEQ={neq} | Proxy={proxy_ip}")
                self.logger.debug(f"VRAC POST request to {second_request_url} for NEQ {neq}")
                
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
                
                headers = self._get_random_headers(referer=response.url, include_user_agent=True)
                headers["Content-Type"] = "application/x-www-form-urlencoded"
                
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
                
                proxy_ip = req_meta.get("proxy", "none").split("://")[1].split(":")[0] if req_meta.get("proxy", "none") != "none" and "://" in req_meta.get("proxy", "") else "none"
                self.logger.debug(f"Making POST to parse_ctq_result | NEQ={neq} | Proxy={proxy_ip}")
                self.logger.debug(f"CTQ POST request to {second_request_url} for NEQ {neq}")
                self.logger.debug(f"Using User-Agent: {headers.get('User-Agent', 'default')[:50]}...")
                
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
        
        # Enhanced debugging for response with prominent status code
        proxy_used = response.meta.get("proxy", "none")
        status = response.status
        body_size = len(response.body) if response.body else 0
        
        # Extract proxy IP for cleaner logging
        proxy_ip = "none"
        if proxy_used != "none" and proxy_used:
            try:
                proxy_ip = proxy_used.split("://")[1].split(":")[0] if "://" in proxy_used else proxy_used.split(":")[0]
            except:
                proxy_ip = proxy_used
        
        self.logger.info(f"parse_ctq_result: status={status} | NEQ={neq} | Proxy={proxy_ip} | BodySize={body_size}")
        
        if status != 200:
            self.logger.warning(f"parse_ctq_result non-200 status {status} for NEQ {neq}")
            return

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
        
        # Enhanced debugging for response with prominent status code
        proxy_used = response.meta.get("proxy", "none")
        status = response.status
        body_size = len(response.body) if response.body else 0
        
        # Extract proxy IP for cleaner logging
        proxy_ip = "none"
        if proxy_used != "none" and proxy_used:
            try:
                proxy_ip = proxy_used.split("://")[1].split(":")[0] if "://" in proxy_used else proxy_used.split(":")[0]
            except:
                proxy_ip = proxy_used
        
        self.logger.info(f"parse_vrac_result: status={status} | NEQ={neq} | Proxy={proxy_ip} | BodySize={body_size}")
        
        if status != 200:
            self.logger.warning(f"parse_vrac_result non-200 status {status} for NEQ {neq}")
            return
        
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
            
            headers = self._get_random_headers(referer=response.url, include_user_agent=True)
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            
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
            
            proxy_ip = req_meta.get("proxy", "none").split("://")[1].split(":")[0] if req_meta.get("proxy", "none") != "none" and "://" in req_meta.get("proxy", "") else "none"
            self.logger.debug(f"Making POST to parse_ctq_result_with_vrac | NEQ={neq} | Proxy={proxy_ip}")
            self.logger.debug(f"CTQ-with-VRAC POST request to {ctq_url} for NEQ {neq}")
            self.logger.debug(f"Using User-Agent: {headers.get('User-Agent', 'default')[:50]}...")
            
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
        
        # Enhanced debugging for response with prominent status code
        proxy_used = response.meta.get("proxy", "none")
        status = response.status
        body_size = len(response.body) if response.body else 0
        
        # Extract proxy IP for cleaner logging
        proxy_ip = "none"
        if proxy_used != "none" and proxy_used:
            try:
                proxy_ip = proxy_used.split("://")[1].split(":")[0] if "://" in proxy_used else proxy_used.split(":")[0]
            except:
                proxy_ip = proxy_used
        
        self.logger.info(f"parse_ctq_result_with_vrac: status={status} | NEQ={neq} | Proxy={proxy_ip} | BodySize={body_size}")
        
        if status != 200:
            self.logger.warning(f"parse_ctq_result_with_vrac non-200 status {status} for NEQ {neq}")
            return

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

    def _iter_neq_values(self, inline_neqs, file_path, start_neq):
        """
        Generator that yields NEQ values one by one, processing inline values first,
        then values from file. Handles deduplication and start_neq resumption.
        Memory-efficient for large files as it processes values incrementally.
        """
        seen = set()
        start_pending = start_neq
        count = 0

        def iterate_sources():
            """Yield values from inline list first, then from file"""
            for val in inline_neqs:
                yield val
            if file_path:
                yield from self._load_neqs_from_file(file_path)

        def generator():
            nonlocal start_pending, count
            for raw in iterate_sources():
                val = str(raw).strip()
                if not val:
                    continue
                
                # Deduplication: skip if we've seen this value
                if val in seen:
                    continue
                seen.add(val)
                count += 1
                
                # Handle start_neq resumption: skip until we find the start value
                if start_pending:
                    if val != start_pending:
                        continue
                    start_pending = None
                    self.logger.info(f"Resuming from NEQ {val} (found at position {count})")
                
                yield val
            
            # Log completion
            if count > 0:
                self.logger.info(f"Generator completed: processed {count} unique NEQ values")

        return generator()

    def _value_exists_in_file(self, file_path: str, target: str) -> bool:
        """
        Check if a target NEQ exists in the file without loading all values.
        Returns as soon as the value is found for efficiency with large files.
        """
        try:
            target_clean = str(target).strip()
            for value in self._load_neqs_from_file(file_path):
                if str(value).strip() == target_clean:
                    return True
        except Exception as exc:
            self.logger.warning(f"Error checking for start_neq in file: {exc}")
            return False
        return False

    def _load_neqs_from_file(self, file_path: str):
        """
        Generator that yields NEQ values from a CSV file one row at a time.
        Memory-efficient for large files as it doesn't load everything into memory.
        """
        try:
            with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                row_count = 0
                for row in reader:
                    row_count += 1
                    val = row.get("NEQ") or next(
                        (v for k, v in row.items() if k and k.lower().strip() == "neq"),
                        None,
                    )
                    if val:
                        stripped = str(val).strip()
                        if stripped:
                            yield stripped
                
                # Log file processing completion
                if row_count > 0:
                    self.logger.debug(f"Processed {row_count} rows from {file_path}")
        except FileNotFoundError:
            self.logger.error(f"File not found: {file_path}")
        except Exception as exc:
            self.logger.warning(f"Failed to load NEQs from {file_path}: {exc}")

    # ------------------------
    # Proxy helpers and request builder
    # ------------------------

    def get_proxy_creds(self, index):
        if not self.proxy_list:
            return {"ip": "", "user": "", "pass": ""}

        entry = self.proxy_list[index % len(self.proxy_list)]
        entry = entry.strip()  # Clean whitespace
        self.logger.debug(f"get_proxy_creds: Raw entry from list: {entry}")
        
        # Determine which separator to use by checking the format
        # Count separators to determine the format
        colon_count = entry.count(":")
        comma_count = entry.count(",")
        
        parts = None
        separator_used = None
        
        # If entry has commas and 3 commas (4 parts), it's comma-separated
        if comma_count == 3:
            parts = [p.strip() for p in entry.split(",")]
            separator_used = "comma"
            self.logger.debug(f"get_proxy_creds: Detected comma-separated format - parts: {parts}, count: {len(parts)}")
        # If entry has colons and 3 colons (4 parts), it's colon-separated
        elif colon_count == 3:
            parts = [p.strip() for p in entry.split(":")]
            separator_used = "colon"
            self.logger.debug(f"get_proxy_creds: Detected colon-separated format - parts: {parts}, count: {len(parts)}")
        # Fallback: try colon first, then comma
        else:
            # Try colon separator first (format: IP:PORT:USER:PASS)
            parts = [p.strip() for p in entry.split(":")]
            if len(parts) == 4:
                separator_used = "colon"
                self.logger.debug(f"get_proxy_creds: Split by colon - parts: {parts}, count: {len(parts)}")
            else:
                # Try comma separator (format: IP,PORT,USER,PASS)
                parts = [p.strip() for p in entry.split(",")]
                if len(parts) == 4:
                    separator_used = "comma"
                    self.logger.debug(f"get_proxy_creds: Split by comma - parts: {parts}, count: {len(parts)}")
        
        # Parse 4-part format (IP:PORT:USER:PASS or IP,PORT,USER,PASS)
        if parts and len(parts) == 4:
            ip, port, user, password = parts
            result = {"ip": f"{ip}:{port}", "user": user, "pass": password}
            self.logger.debug(f"get_proxy_creds: Parsed proxy - ip:port={result['ip']}, user={result['user']}, format={separator_used}")
            return result
        
        # Fallback: try to parse as IP:PORT (no auth) with either separator
        if parts and len(parts) == 2:
            ip, port = parts
            result = {"ip": f"{ip}:{port}", "user": "", "pass": ""}
            self.logger.debug(f"get_proxy_creds: Parsed as IP:PORT (no auth) - {result['ip']}")
            return result
        
        # Last resort: use entry as-is
        result = {"ip": entry, "user": "", "pass": ""}
        self.logger.warning(f"get_proxy_creds: Could not parse proxy entry (colons={colon_count}, commas={comma_count}), using as-is: {result['ip']}")
        return result

    def _next_proxy(self):
        if not self.proxy_list:
            return {"ip": "", "user": "", "pass": ""}
        # Increment index and get proxy
        self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
        self.logger.debug(f"_next_proxy: Rotating to index {self.current_proxy_index} (total proxies: {len(self.proxy_list)})")
        result = self.get_proxy_creds(self.current_proxy_index)
        self.logger.debug(f"_next_proxy: Returning proxy - ip:port={result['ip']}, has_auth={bool(result['user'] and result['pass'])}")
        return result
    
    def _get_current_proxy(self):
        """Get current proxy without incrementing (for preview/logging)"""
        if not self.proxy_list:
            return {"ip": "", "user": "", "pass": ""}
        next_index = (self.current_proxy_index + 1) % len(self.proxy_list)
        return self.get_proxy_creds(next_index)
    
    def _get_random_headers(self, referer=None, include_user_agent=True):
        """Generate random headers with rotating user agent and accept-language"""
        user_agent = random.choice(self.user_agents) if include_user_agent else None
        accept_language = random.choice(self.accept_languages)
        
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": accept_language,
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin" if referer else "none",
            "Cache-Control": "max-age=0",
        }
        
        if include_user_agent and user_agent:
            headers["User-Agent"] = user_agent
        
        if referer:
            headers["Referer"] = referer
            headers["Origin"] = "https://www.pes.ctq.gouv.qc.ca"
        
        return headers

    @staticmethod
    def _format_excel_text(value: str):
        """Prefix value to prevent Excel from auto-formatting"""
        if not value:
            return ""
        return f"{value}"

    def make_request(self, url, callback, meta=None, method="GET", formdata=None):
        referer = meta.get("referer") if meta else None
        headers = self._get_random_headers(referer=referer, include_user_agent=True)

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

        neq = req.meta.get("neq", "unknown")
        proxy_used = req.meta.get("proxy", "none")
        
        proxy_ip = "none"
        if proxy_used != "none" and proxy_used:
            try:
                proxy_ip = proxy_used.split("://")[1].split(":")[0] if "://" in proxy_used else proxy_used.split(":")[0]
            except:
                proxy_ip = proxy_used
        
        failure_type = type(failure.value).__name__ if hasattr(failure, 'value') and failure.value else type(failure).__name__
        failure_msg = str(failure.value) if hasattr(failure, 'value') and failure.value else str(failure)
        
        response_status = "N/A"
        if hasattr(failure, 'response') and failure.response:
            response_status = failure.response.status
        
        self.logger.error(f"ERROR: NEQ={neq} | Proxy={proxy_ip} | Status={response_status} | Error={failure_type}: {failure_msg}")
        self.errors += 1
