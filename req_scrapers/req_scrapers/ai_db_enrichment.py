import os
import sys
import time
from typing import List, Dict, Any, Optional

import pymysql
from dotenv import load_dotenv

# Re‑use the existing enrichment logic (prompt + OpenAI call)
from req_scrapers.ai_enhancment import enrich_company


load_dotenv()


def get_mysql_connection() -> pymysql.connections.Connection:
    """
    Create a MySQL connection using the same defaults as Scrapy settings.
    Falls back to env vars and finally hardcoded defaults.
    """
    host = os.getenv("MYSQL_HOST", "52.60.176.24")
    db = os.getenv("MYSQL_DB", "v1lead2424_REQ_DB")
    user = os.getenv("MYSQL_USER", "v1lead2424_saad")
    password = os.getenv("MYSQL_PASSWORD", "1g2jWMb$WE^7HLe0")
    port = int(os.getenv("MYSQL_PORT", "3306"))

    return pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=db,
        port=port,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )


def fetch_pending_rows(
    conn: pymysql.connections.Connection, limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Load rows from ai_test that are missing telephone and/or website.
    """
    sql = """
        SELECT
            id,
            neq,
            nom,
            full_address,
            adresse,
            ville,
            province,
            code_postal,
            telephone,
            website
        FROM ai_test
        WHERE (telephone IS NULL OR telephone = '')
           OR (website IS NULL OR website = '')
        ORDER BY id ASC
    """
    if limit and limit > 0:
        sql += " LIMIT %s"
        params = (limit,)
    else:
        params = ()

    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        rows = cursor.fetchall()

    return rows or []


def build_company_payload(row: Dict[str, Any]) -> Dict[str, str]:
    """
    Build the minimal payload expected by enrich_company()
    using columns from ai_test.
    """
    adresse = (row.get("adresse") or "").strip()
    if not adresse:
        # Fall back to full_address if adresse is empty
        adresse = (row.get("full_address") or "").strip()

    return {
        "neq": str(row.get("neq") or "").strip(),
        "nom": (row.get("nom") or "").strip(),
        "adresse": adresse,
        "ville": (row.get("ville") or "").strip(),
        "province": (row.get("province") or "").strip(),
        "code_postal": (row.get("code_postal") or "").strip(),
    }


def update_ai_test_row(
    conn: pymysql.connections.Connection,
    row_id: int,
    telephone: str,
    website: str,
) -> None:
    """
    Update telephone and website columns in ai_test.
    """
    sql = """
        UPDATE ai_test
        SET telephone = %s,
            website = %s
        WHERE id = %s
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (telephone or "", website or "", row_id))


def enrich_ai_test_rows(limit: Optional[int] = None, commit_every: int = 5) -> None:
    """
    Main worker: read incomplete rows from ai_test, call enrich_company()
    using name + address, and write back telephone and website.
    """
    try:
        conn = get_mysql_connection()
    except Exception as e:
        raise SystemExit(f"Failed to connect to MySQL: {e}")

    try:
        rows = fetch_pending_rows(conn, limit=limit)
        if not rows:
            print("No rows in ai_test need enrichment (telephone/website).")
            return

        print(f"Found {len(rows)} row(s) in ai_test needing enrichment.")

        processed = 0
        for idx, row in enumerate(rows, start=1):
            row_id = row.get("id")
            print(
                f"[{idx}/{len(rows)}] Enriching id={row_id} | NEQ={row.get('neq')} | NOM={row.get('nom')}",
                flush=True,
            )

            company_payload = build_company_payload(row)

            try:
                enriched = enrich_company(company_payload) or {}
            except Exception as e:
                print(f"  !! AI enrichment failed for id={row_id}: {e}", flush=True)
                continue

            telephone = (enriched.get("phone_number") or "").strip()
            website = (enriched.get("company_website") or "").strip()

            try:
                update_ai_test_row(conn, row_id=row_id, telephone=telephone, website=website)
                processed += 1
            except Exception as e:
                print(f"  !! DB update failed for id={row_id}: {e}", flush=True)
                conn.rollback()
                continue

            if processed % commit_every == 0:
                try:
                    conn.commit()
                    print(f"Committed {processed} updated row(s) so far.", flush=True)
                except Exception as e:
                    print(f"  !! Commit failed after {processed} rows: {e}", flush=True)
                    conn.rollback()

            # Be gentle with the API; tiny pause between calls.
            time.sleep(0.5)

        # Final commit
        try:
            conn.commit()
        except Exception as e:
            print(f"Final commit failed: {e}", flush=True)
            conn.rollback()

        print(f"Done. Successfully updated {processed} row(s) in ai_test.")

    finally:
        try:
            conn.close()
        except Exception:
            pass


def _parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        iv = int(value)
        return iv if iv > 0 else None
    except ValueError:
        return None


if __name__ == "__main__":
    # Usage:
    #   python -m req_scrapers.ai_db_enrichment [max_rows]
    #
    # Example:
    #   python -m req_scrapers.ai_db_enrichment 50
    #
    max_rows = _parse_int(sys.argv[1]) if len(sys.argv) > 1 else None
    enrich_ai_test_rows(limit=max_rows)


