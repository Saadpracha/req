
import os
import re
import json
import csv
import sys
import requests
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI  # official SDK

# Load .env
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise SystemExit("ERROR: OPENAI_API_KEY not found in environment (.env).")

# Init client (the SDK will read from env if you prefer `OpenAI()`)
client = OpenAI(api_key=OPENAI_API_KEY)

# Prompting template: instruct model to return strict JSON conforming to schema
SYSTEM_PROMPT = """
You are a data-enrichment assistant. Given a company's canonical information (name, address, city, province, postal code),
return a single JSON object (no extra text) that exactly matches this schema:

{
  "nom": string,                 # original company name (do NOT translate)
  "adresse": string,             # original address (do NOT translate)
  "ville": string,               # original city (do NOT translate)
  "province": string,            # original province (do NOT translate)
  "code_postal": string,         # original postal code (do NOT translate)
  "phone_number": string,        # best phone number found (E.164 format if possible) or empty string if not found
  "phone_number_source": string, # URL or webpage where phone number was found, or empty string if not found
  "reliability_level": int,      # must be either 0 or 10 — 0 = no reliable source found, 10 = verified from an official or highly trusted source
  "company_website": string,     # official company website, or empty string if not found
  "contacts": [                  # array of contact objects; empty array if none found
    {
      "first_name": string,      # contact's first name (keep as-is; no translation; empty if unknown)
      "last_name": string,       # contact's last name (keep as-is; no translation; empty if unknown)
      "title": string,           # contact's title (translate to English if not in English; empty if unknown)
      "source": string           # website or webpage where this contact info was found; empty if not available
    }
  ],
  "notes": string                # short but detailed explanation. Describe reliability reasoning and mention key data sources used.
}

Rules:
- Return ONLY valid JSON matching the schema above — no extra text, comments, or explanations.
- All output must be in English.
- Translate any non-English data to English, except for the original input fields and contact first/last names.
- Never modify or translate the input fields (nom, adresse, ville, province, code_postal).
- reliability_level can only be 0 or 10:
    • 0 = no reliable or verifiable source found.
    • 10 = data confirmed from an official company website, government record, or authoritative business directory.
- Always specify the source for the phone number and for each contact (URL or webpage if available).
- Use empty strings ("") for missing or unavailable values instead of null.
- The "notes" field must include a brief explanation of how reliability was determined and the primary sources referenced.
"""



def extract_json_from_text(text: str):
    """
    Attempt to extract JSON object from a possibly noisy model response.
    """
    # Try to find the first { ... } block
    match = re.search(r'(\{.*\})', text, re.DOTALL)
    if match:
        candidate = match.group(1)
        try:
            return json.loads(candidate)
        except json.JSONDecodeError as e:
            # Try to fix common model quirks: replace single quotes, trailing commas
            s = candidate.replace("'", '"')
            s = re.sub(r',\s*([}\]])', r'\1', s)  # remove trailing commas
            try:
                return json.loads(s)
            except json.JSONDecodeError:
                # If still failing, try to fix truncated JSON by adding missing closing braces
                try:
                    # Count opening and closing braces
                    open_braces = s.count('{')
                    close_braces = s.count('}')
                    if open_braces > close_braces:
                        # Add missing closing braces
                        missing_braces = open_braces - close_braces
                        s += '}' * missing_braces
                        return json.loads(s)
                except json.JSONDecodeError:
                    pass
                # If all else fails, show the error with context
                print(f"JSON parsing failed. Error: {e}")
                print(f"Problematic JSON (first 500 chars): {candidate[:500]}")
                raise
    # If nothing found, try to load entire text
    return json.loads(text)

def enrich_company(company: dict):
    user_content = (
        "Enrich this company record. Return the JSON strictly as requested above.\n\n"
        f"INPUT:\n{json.dumps(company, ensure_ascii=False)}\n\n"
        "Only output the JSON object."
    )

    # Call Responses API directly to enable web_search tool (SDK may not support .responses yet)
    url = "https://api.openai.com/v1/responses"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "gpt-4.1-mini",
        "input": f"{SYSTEM_PROMPT}\n\n{user_content}",
        "tools": [{"type": "web_search"}],
        "temperature": 0.0,
        "max_output_tokens": 2000,
    }
    try:
        http_resp = requests.post(url, headers=headers, json=payload, timeout=180)
        http_resp.raise_for_status()
        data = http_resp.json()
    except requests.exceptions.Timeout:
        raise SystemExit(f"OpenAI API request timed out after 180 seconds. This may be due to high API load. Please try again later.")
    except requests.exceptions.RequestException as e:
        raise SystemExit(f"OpenAI API request failed: {e}")
    except Exception as e:
        raise SystemExit(f"Unexpected error during API request: {e}")

    # Extract assistant text robustly from Responses API payload
    text = None
    if isinstance(data, dict):
        # Preferred field on Responses API
        text = data.get("output_text")

        # Fallbacks
        if not text and "output" in data:
            output_field = data["output"]
            # If the API returned a direct string
            if isinstance(output_field, str):
                text = output_field
            # If the API returned a list of items
            elif isinstance(output_field, list):
                pieces = []
                for item in output_field:
                    # Some SDKs may return strings in the list
                    if isinstance(item, str):
                        pieces.append(item)
                        continue
                    if not isinstance(item, dict):
                        continue
                    content_list = item.get("content")
                    if isinstance(content_list, list):
                        for c in content_list:
                            if isinstance(c, str):
                                pieces.append(c)
                                continue
                            if not isinstance(c, dict):
                                continue
                            c_type = c.get("type")
                            if c_type == "output_text":
                                text_obj = c.get("text")
                                if isinstance(text_obj, dict):
                                    val = text_obj.get("value")
                                    if isinstance(val, str) and val.strip():
                                        pieces.append(val)
                                elif isinstance(text_obj, str) and text_obj.strip():
                                    pieces.append(text_obj)
                                continue
                            # generic text shape
                            if "text" in c:
                                val_field = c.get("text")
                                if isinstance(val_field, dict):
                                    val = val_field.get("value")
                                    if isinstance(val, str) and val.strip():
                                        pieces.append(val)
                                elif isinstance(val_field, str) and val_field.strip():
                                    pieces.append(val_field)
                if pieces:
                    text = "\n".join(pieces).strip()

        # Legacy-style fallback similar to chat.completions
        if not text and isinstance(data.get("choices"), list) and data["choices"]:
            choice0 = data["choices"][0]
            if isinstance(choice0, dict):
                msg = choice0.get("message")
                if isinstance(msg, dict):
                    content = msg.get("content")
                    if isinstance(content, str):
                        text = content

    if not text:
        # Help debugging by showing a compact preview of the response
        preview = data
        try:
            preview = json.dumps(data)[:1000]
        except Exception:
            pass
        raise SystemExit(f"Unexpected API response format: could not extract assistant text. Preview: {preview}")
    # Try best-effort to parse JSON
    try:
        result = extract_json_from_text(text)
    except Exception as e:
        print("Failed to parse JSON from model response. Raw response below:")
        print(text)
        print("\nAttempting to create a minimal valid response...")
        
        # Create a minimal valid response with available data
        try:
            # Try to extract at least the basic company info from the truncated response
            result = {
                "nom": company.get("nom", ""),
                "adresse": company.get("adresse", ""),
                "ville": company.get("ville", ""),
                "province": company.get("province", ""),
                "code_postal": company.get("code_postal", ""),
                "phone_number": "",
                "phone_number_source": "",
                "reliability_level": 0,
                "company_website": "",
                "contacts": [],
                "notes": f"JSON parsing failed due to truncated response. Original error: {str(e)}"
            }
            print("Created minimal response due to JSON parsing failure.")
        except Exception as fallback_error:
            raise SystemExit(f"JSON parsing error: {e}. Fallback also failed: {fallback_error}")

    # Basic validation / normalization
    # Ensure required keys exist
    schema_keys = ["nom","adresse","ville","province","code_postal","phone_number","reliability_level","company_website","contacts","notes"]
    for k in schema_keys:
        if k not in result:
            # If model omitted keys, add them with null/defaults
            result.setdefault(k, None if k not in ("contacts",) else [])
    # Ensure contacts is an array
    if result.get("contacts") is None:
        result["contacts"] = []
    return result

def load_companies_from_csv(file_path: str):
    """
    Load companies from CSV file with columns: nom, adresse, ville, province, code_postal
    """
    companies = []
    try:
        with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row:
                    continue
                
                # Extract the required columns
                company = {
                    "nom": row.get("company", "").strip(),
                    "adresse": row.get("address", "").strip(),
                    "ville": row.get("city", "").strip(),
                    "province": row.get("state", "").strip(),
                    "code_postal": row.get("postal_code", "").strip()
                }
                # Optional gating column: only enrich when value is "Oui"
                # Check if droit_circulation is in other_fields JSON
                other_fields = row.get("other fields", "").strip()
                if other_fields:
                    try:
                        import json
                        other_data = json.loads(other_fields)
                        company["droit_circulation"] = other_data.get("droit_circulation", "").strip()
                    except (json.JSONDecodeError, AttributeError):
                        company["droit_circulation"] = ""
                else:
                    company["droit_circulation"] = ""
                
                # Only add if at least nom is present
                if company["nom"]:
                    companies.append(company)
                    
    except Exception as e:
        print(f"Error reading CSV file {file_path}: {e}")
        return []
    
    return companies

def process_csv_file(input_file: str, output_file: str = None, max_companies: int = None):
    """
    Process companies from CSV file and save enriched results
    Args:
        input_file: Path to input CSV file
        output_file: Path to output JSON file
        max_companies: Maximum number of companies to process (None for all)
    """
    companies = load_companies_from_csv(input_file)
    if not companies:
        print(f"No companies found in {input_file}")
        return
    
    # Limit companies if max_companies is specified
    if max_companies and max_companies > 0:
        companies = companies[:max_companies]
        print(f"Processing {len(companies)} companies (limited to {max_companies}) from {input_file}")
    else:
        print(f"Processing {len(companies)} companies from {input_file}")
    
    # Load existing results if output file exists
    enriched_companies = []
    if output_file and Path(output_file).exists():
        try:
            with open(output_file, "r", encoding="utf-8-sig") as f:
                enriched_companies = json.load(f)
            print(f"Loaded {len(enriched_companies)} existing results from {output_file}")
        except Exception as e:
            print(f"Warning: Could not load existing results: {e}")
    
    # Save results
    if not output_file:
        output_file = f"enriched_{Path(input_file).stem}.json"
    
    processed_count = len(enriched_companies)
    
    for i, company in enumerate(companies, 1):
        print(f"Processing {i}/{len(companies)}: {company['nom']}")
        
        # Check if this company has already been processed
        company_already_processed = False
        for existing_company in enriched_companies:
            if (existing_company.get("nom", "").strip().lower() == company.get("nom", "").strip().lower() and
                existing_company.get("adresse", "").strip().lower() == company.get("adresse", "").strip().lower()):
                print(f"Already processed: {company['nom']} - skipping")
                company_already_processed = True
                break
        
        if company_already_processed:
            continue
            
        # Skip enrichment when gating column exists and is not "Oui"
        dc_val = (company.get("droit_circulation") or "").strip().casefold()
        if dc_val and dc_val != "oui":
            print(f"Skipping {company['nom']} (droit_circulation={company.get('droit_circulation')})")
            # Do not append anything for skipped rows
            continue
        try:
            enriched = enrich_company(company)
            enriched_companies.append(enriched)
            processed_count += 1
            
            # Save incrementally every 5 companies to prevent data loss
            if processed_count % 5 == 0:
                with open(output_file, "w", encoding="utf-8-sig", newline="", errors='replace') as f:
                    json.dump(enriched_companies, f, ensure_ascii=False, indent=2)
                print(f"Progress saved: {processed_count} companies processed so far")
                
        except Exception as e:
            print(f"Error enriching {company['nom']}: {e}")
            # Save current progress even on error
            try:
                with open(output_file, "w", encoding="utf-8-sig", newline="", errors='replace') as f:
                    json.dump(enriched_companies, f, ensure_ascii=False, indent=2)
                print(f"Progress saved after error: {processed_count} companies processed so far")
            except Exception as save_error:
                print(f"Warning: Could not save progress after error: {save_error}")
            continue
    
    # Final save
    with open(output_file, "w", encoding="utf-8-sig", newline="", errors='replace') as f:
        json.dump(enriched_companies, f, ensure_ascii=False, indent=2)
    
    print(f"Enrichment completed. Results saved to {output_file}")
    print(f"Successfully processed {len(enriched_companies)} companies")

if __name__ == "__main__":
    # Require CSV file as command line argument
    if len(sys.argv) < 2:
        print("Usage: python ai_enhancment.py <input_csv_file> [output_json_file] [max_companies]")
        print("CSV file must contain columns: company, address, city, state, postal_code")
        print("max_companies: Optional limit on number of companies to process")
        sys.exit(1)
    
    input_csv = sys.argv[1]
    output_json = sys.argv[2] if len(sys.argv) > 2 else None
    max_companies = None
    
    # Parse max_companies if provided
    if len(sys.argv) > 3:
        try:
            max_companies = int(sys.argv[3])
            if max_companies <= 0:
                print("Error: max_companies must be a positive integer")
                sys.exit(1)
        except ValueError:
            print("Error: max_companies must be a valid integer")
            sys.exit(1)
    
    # Check if input file exists
    if not Path(input_csv).exists():
        print(f"Error: Input file '{input_csv}' not found.")
        sys.exit(1)
    
    process_csv_file(input_csv, output_json, max_companies)
