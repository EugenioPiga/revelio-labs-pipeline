import os
import requests
from tqdm import tqdm
# Output directory on server
OUTPUT_DIR = "/labs/khanna/USPTO_202511"
os.makedirs(OUTPUT_DIR, exist_ok=True)
# Bulk dataset files from PatentsView
DATASETS = {
    "g_us_patent_citation": "https://s3.amazonaws.com/data.patentsview.org/download/g_us_patent_citation.tsv.zip",
    "g_foreign_citation": "https://s3.amazonaws.com/data.patentsview.org/download/g_foreign_citation.tsv.zip",
    "g_cpc_current": "https://s3.amazonaws.com/data.patentsview.org/download/g_cpc_current.tsv.zip",
    "g_assignee_disambiguated": "https://s3.amazonaws.com/data.patentsview.org/download/g_assignee_disambiguated.tsv.zip",
    "g_inventor_disambiguated": "https://s3.amazonaws.com/data.patentsview.org/download/g_inventor_disambiguated.tsv.zip",
    "g_patent": "https://s3.amazonaws.com/data.patentsview.org/download/g_patent.tsv.zip"
}
def download_file(url, output_path):
    """Stream download with progress bar"""
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    total_size = int(resp.headers.get("content-length", 0))
    with open(output_path, "wb") as f, tqdm(
        total=total_size, unit="B", unit_scale=True, desc=os.path.basename(output_path)
    ) as bar:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
                bar.update(len(chunk))
def main():
    for name, url in DATASETS.items():
        out_path = os.path.join(OUTPUT_DIR, f"{name}.tsv.zip")
        if os.path.exists(out_path):
            print(f"[SKIP] {out_path} already exists.")
            continue
        print(f"[INFO] Downloading {name} ...")
        try:
            download_file(url, out_path)
            print(f"[DONE] {name} saved to {out_path}")
        except Exception as e:
            print(f"[ERROR] Failed {name}: {e}")
if __name__ == "__main__":
    main()