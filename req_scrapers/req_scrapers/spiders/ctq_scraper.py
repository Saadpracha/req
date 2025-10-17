import csv
import scrapy
from scrapy.http import FormRequest
from urllib.parse import urljoin
from datetime import datetime


class CtqScraperSpider(scrapy.Spider):
    name = "ctq_scraper"
    allowed_domains = ["pes.ctq.gouv.qc.ca"]
    start_urls = ["https://www.pes.ctq.gouv.qc.ca/pes2/mvc/dossierclient"]
    custom_settings = {
        "LOG_LEVEL": "DEBUG",
    }

    def __init__(self, neqs=None, file=None, start_neq=None, *args, **kwargs):
        """
        Args:
            neqs: comma-separated NEQ list
            file: CSV file path containing NEQ column
            start_neq: optional NEQ value to resume scraping from
        """
        super().__init__(*args, **kwargs)
        self.neqs = neqs.split(",") if neqs else []
        if file:
            self.neqs.extend(self._load_neqs_from_file(file))

        # Clean and deduplicate NEQs
        self.neqs = [str(n).strip() for n in self.neqs if str(n).strip()]
        self.neqs = list(dict.fromkeys(self.neqs))  # preserve order

        # Resume from a specific NEQ if provided
        if start_neq and start_neq in self.neqs:
            start_index = self.neqs.index(start_neq)
            self.neqs = self.neqs[start_index:]
            self.logger.info(f"Resuming from NEQ {start_neq} (index {start_index})")
        elif start_neq:
            self.logger.warning(f"Start NEQ {start_neq} not found in file. Running from first value.")

    def start_requests(self):
        for neq in self.neqs:
            yield scrapy.Request(
                url=self.start_urls[0],
                callback=self.parse_initial,
                meta={"neq": neq, "cookiejar": f"jar-{neq}"},
                dont_filter=True,
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
        return FormRequest(
            url=post_url,
            formdata=formdata,
            callback=self.check_validity,
            meta={"neq": neq, "cookiejar": response.meta.get("cookiejar")},
            dont_filter=True,
        )

    def check_validity(self, response):
        neq = response.meta["neq"]
        ctq_action = response.xpath('//form[@id="mainForm"]/@action').get()
        if ctq_action:
            ctq_final_url = urljoin("https://www.pes.ctq.gouv.qc.ca", ctq_action)

            if response.xpath('//h6[contains(text(),"Erreur(s)")]'):
                return  # Invalid NEQ

            match_text = response.xpath('//acronym/following-sibling::p/text()').get()
            if match_text == neq:
                yield FormRequest(
                    url=ctq_final_url,
                    formdata=self.extract_form_data(response),
                    callback=self.parse_ctq_result,
                    meta={"neq": neq, "cookiejar": response.meta.get("cookiejar")},
                    dont_filter=True
                )

    def extract_form_data(self, response):
        pecvl_onclick = response.xpath("//a[contains(@onclick, 'PECVL')]/@onclick").get()
        params = {}
        target_id = None

        if pecvl_onclick:
            try:
                parts = pecvl_onclick.split("submitForm(")[1].split(")")[0]
                pre, post = parts.split(",null,")
                target_id = pre.split(",")[1].strip().strip("'\"")
                params_str = post.strip().strip("[]")
                rows = params_str[1:-1].split("],[")
                for row in rows:
                    key, value = row.replace("'", "").split(",")
                    params[key] = value
            except Exception:
                pass

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
            "leClientNo": params.get("leClientNo", "129540"),
            "leContexte": params.get("leContexte", "PECVL"),
            "leOrderBy": params.get("leOrderBy", ""),
            "leOrderDir": params.get("leOrderDir", ""),
            "leContexteEstDejaDetermine": params.get("leContexteEstDejaDetermine", "oui"),
            "leDdrSeq": params.get("leDdrSeq", "0"),
            "mainForm:_idcl": target_id or "mainForm:j_id_z_7_2",
        }
        return data

    def parse_ctq_result(self, response):
        neq = response.meta["neq"]
        table_xpath = '//table[contains(@class, "topTableauFixe")]'
        if not response.xpath(table_xpath):
            return

        def extract_text(xpath):
            return response.xpath(xpath).get(default="").strip()

        def normalize(p_list):
            return [p.strip() for p in p_list if p.strip()]

        def format_date(value: str):
            """Normalize date to YYYY-MM-DD or YYYY-MM-DD 00:00"""
            if not value:
                return ""
            value = value.strip()
            for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d", "%d %B %Y", "%d %b %Y"):
                try:
                    parsed = datetime.strptime(value, fmt)
                    return parsed.strftime("%Y-%m-%d")
                except ValueError:
                    continue
            # If includes time-like patterns
            try:
                parsed = datetime.fromisoformat(value)
                return parsed.strftime("%Y-%m-%d %H:%M")
            except Exception:
                pass
            return value  # leave unchanged if format not detected

        full_address_raw = response.xpath("//strong[normalize-space(.)=\"Adresse d'affaires\"]/following-sibling::p//text()").getall()
        full_address_lines = normalize(full_address_raw)

        adresse = full_address_lines[0] if full_address_lines else ""
        ville, province, code_postal = "", "", ""
        if len(full_address_lines) > 1:
            try:
                city_part = full_address_lines[1].split("(")[0].strip()
                province_part = full_address_lines[1].split("(")[1].split(")")[0].strip()
                postal_code_part = full_address_lines[1].split(")")[-1].strip()
                ville, province, code_postal = city_part, province_part, postal_code_part
            except Exception:
                pass

        neq_text = response.xpath('//acronym[@title="Numéro d\'entreprise du Québec"]/following-sibling::p/text()').get()
        if neq_text:
            neq_text = neq_text.strip()

        nir_list = response.xpath('(//acronym[@title="Numéro d\'identification au Registre"])[1]/following-sibling::p[1]/text()').getall()
        nir = ""
        for val in nir_list:
            val = val.strip()
            if val.startswith("R-") and val.count("-") >= 2:
                nir = val
                break

        yield {
            "neq": neq_text or neq,
            "nom": extract_text("//strong[normalize-space(.)='Nom']/following-sibling::p/text()"),
            "full_address": " ".join(full_address_lines),
            "adresse": adresse,
            "ville": ville,
            "province": province,
            "code_postal": code_postal,
            "nir": nir,
            "titre": extract_text("//strong[normalize-space(.)='Titre']/following-sibling::p/text()"),
            "categorie_transport": extract_text("//strong[normalize-space(.)='Catégorie de transport']/following-sibling::p/text()"),
            "date_inscription": format_date(extract_text("//strong[normalize-space(.)=\"Date d'inscription au registre\"]/following-sibling::p/text()")),
            "date_prochaine_maj": format_date(extract_text("//strong[normalize-space(.)='Date limite de la prochaine mise à jour']/following-sibling::p/text()")),
            "code_securite": extract_text("//strong[normalize-space(.)='Cote de sécurité']/following-sibling::p/text()"),
            "droit_circulation": extract_text("//strong[normalize-space(.)='Droit de mettre en circulation (Propriétaire)']/following-sibling::p/text()"),
            "droit_exploiter": extract_text("//strong[normalize-space(.)=\"Droit d'exploiter (Exploitant)\"]/following-sibling::p/text()"),
            "motif": extract_text("//strong[normalize-space(.)='Motif']/following-sibling::p//text()[1]"),
        }

    def _load_neqs_from_file(self, file_path: str):
        values = []
        try:
            with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if not row:
                        continue
                    val = row.get("NEQ")
                    if val is None:
                        for k, v in row.items():
                            if k and k.lower().strip() == "neq":
                                val = v
                                break
                    if val is not None:
                        s = str(val).strip()
                        if s:
                            values.append(s)
        except Exception as e:
            self.logger.warning(f"Error loading NEQs from file: {e}")
        return values
