import asyncio
import os
import sys
import httpx
import json

# Ensure we can import from app
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.config import settings

_APIFY_BASE = "https://api.apify.com/v2"

async def main():
    target_url = "https://www.autopunditz.com/latest-posts"
    actor_id = "apify/website-content-crawler"
    actor_path = actor_id.replace("/", "~")
    
    token = settings.apify_token or os.environ.get("APIFY_TOKEN", "")
    if not token:
        print("Error: No Apify token found in settings or environment.")
        return

    print(f"Triggering {actor_id} for {target_url}...")
    run_input = {
        "startUrls": [{"url": target_url}],
        "maxCrawlPages": 1,
        "crawlerType": "playwright:firefox", # Often necessary for modern JS/Wix sites
        "saveHtml": False,
        "saveMarkdown": True # website-content-crawler usually supports this, very clean
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                f"{_APIFY_BASE}/acts/{actor_path}/runs",
                params={"token": token},
                json=run_input,
            )
            r.raise_for_status()
            run_data = r.json()["data"]
            run_id = run_data["id"]
            print(f"Started Apify run: {run_id}")
    except Exception as e:
        print(f"Error starting Apify actor: {e}")
        return

    print("Waiting for dataset to be ready (this may take a minute or two)...")
    dataset_id = None
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            for _ in range(60): # 60 * 10s = 10 minutes max
                await asyncio.sleep(10)
                r = await client.get(
                    f"{_APIFY_BASE}/actor-runs/{run_id}",
                    params={"token": token},
                )
                r.raise_for_status()
                data = r.json()["data"]
                status = data["status"]
                print(f"Status: {status}")
                if status == "SUCCEEDED":
                    dataset_id = data["defaultDatasetId"]
                    break
                if status in ("FAILED", "ABORTED", "TIMED-OUT"):
                    print(f"Apify run {run_id} ended with {status}")
                    return
    except Exception as e:
        print(f"Error polling Apify run: {e}")
        return

    if not dataset_id:
        print("Timed out waiting for dataset.")
        return

    print(f"Fetching dataset {dataset_id}...")
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.get(
                f"{_APIFY_BASE}/datasets/{dataset_id}/items",
                params={"token": token, "format": "json"},
            )
            r.raise_for_status()
            items = r.json()
            
            # Save raw dataset to file
            out_json = os.path.join(os.path.dirname(__file__), "apify_custom_site_raw.json")
            with open(out_json, "w", encoding="utf-8") as f:
                json.dump(items, f, indent=2, ensure_ascii=False)
            
            if items:
                # Format output nicely
                item = items[0]
                text = item.get("markdown") or item.get("text") or "No text found"
                title = item.get("metadata", {}).get("title") or "No title"
                
                out_path = os.path.join(os.path.dirname(__file__), "apify_custom_site_result.txt")
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(f"Title: {title}\n")
                    f.write(f"URL: {target_url}\n")
                    f.write(f"Length: {len(text)} characters\n\n")
                    f.write("-" * 40 + "\n")
                    f.write(text)
                    
                print(f"\nSuccess! Found text of length {len(text)}")
                print(f"Saved results to: {out_path}")
            else:
                print("Dataset was empty. The crawler found no content.")
    except Exception as e:
        print(f"Error fetching dataset: {e}")

if __name__ == "__main__":
    asyncio.run(main())
