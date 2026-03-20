import os
import asyncio
import aiohttp
import aiosqlite
import aioboto3
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from io import BytesIO

# --- CONFIGURATION CLOUDFLARE R2 ---
R2_ACCOUNT_ID = "b6c7083b2dda14cf990b9f8a3807e72d"
R2_ACCESS_KEY = "d29ad6a70b9c8e139bcdadd704712473"
R2_SECRET_KEY = "78ea3942a5d9699851e1a242f68b56221ec6dca21dfac45f509b0658af20914f"
R2_BUCKET_NAME = "materials"
R2_ENDPOINT_URL = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

# --- CONFIGURATION CRAWLER ---
BASE_URL = "https://sujetexa.com"
DB_NAME = "crawler_state.db"
EXTENSIONS = {'.pdf', '.zip'}
MAX_RETRIES = 2 
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/119.0.0.0",
    "Accept-Encoding": "gzip, deflate",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
}


def load_list_from_file(filepath):
    """Charge chaque ligne d'un fichier dans un ensemble (set) pour une recherche rapide."""
    if not os.path.exists(filepath):
        print(f"Attention : Le fichier {filepath} est introuvable.")
        return set()
    with open(filepath, 'r', encoding='utf-8') as f:
        # strip() enlève les espaces/sauts de ligne, if line évite les lignes vides
        return {line.strip() for line in f if line.strip()}

BLACKLIST = load_list_from_file("blacklist.txt")
CATEGORIES = load_list_from_file("categories.txt")

class R2Crawler:
    def __init__(self):
        self.domain = urlparse(BASE_URL).netloc
        self.session_r2 = aioboto3.Session(
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY
        )

    async def init_db(self):
        self.db = await aiosqlite.connect(DB_NAME)
        await self.db.execute('''
            CREATE TABLE IF NOT EXISTS urls (
                url TEXT PRIMARY KEY,
                status TEXT, -- 'visited', 'uploaded', 'failed'
                http_code INTEGER,
                attempts INTEGER DEFAULT 0
            )
        ''')
        await self.db.commit()

    def detect_category(self, history):
        """Remonte l'historique des URLs pour trouver la première catégorie valide."""
        for url in reversed(history):
            url_lower = url.lower()
            for cat in CATEGORIES:
                if cat.lower() in url_lower:
                    return cat
        return ""

    async def is_processed(self, url):
        async with self.db.execute("SELECT status FROM urls WHERE url = ?", (url,)) as cursor:
            row = await cursor.fetchone()
            return row is not None and row[0] in ['visited', 'uploaded']

    async def upload_to_r2(self, file_data, filename, category):
        """Upload le contenu binaire vers Cloudflare R2."""
        async with self.session_r2.client("s3", endpoint_url=R2_ENDPOINT_URL) as s3:
            key = f"{category}/{filename}" if category else filename
            print(f"Uploading {filename} to R2 as {key}")
            await s3.upload_fileobj(BytesIO(file_data), R2_BUCKET_NAME, key)

    async def process_file(self, session, url, history):
        if await self.is_processed(url): return
        
        status, code = "failed", 0
        category = self.detect_category(history)
        for i in range(MAX_RETRIES + 1):
            try:
                async with session.get(url, timeout=30) as resp:
                    code = resp.status
                    if code == 200:
                        content = await resp.read()
                        filename = os.path.basename(urlparse(url).path)
                        await self.upload_to_r2(content, filename, category)
                        status = "uploaded"
                        break
            except Exception:
                code = 999
            await asyncio.sleep(1)

        await self.db.execute("INSERT OR REPLACE INTO urls VALUES (?, ?, ?, ?)", (url, status, code, i+1))
        await self.db.commit()
        print(f"  [{status.upper()}] {url} (HTTP: {code})")

    async def crawl(self, session, queue):
        while not queue.empty():
            current_url, history = await queue.get()
            if any(word in current_url.lower() for word in BLACKLIST):
                print(f"🚫 BLACKLIST : {current_url}")
                queue.task_done()
                continue

            if await self.is_processed(current_url):
                queue.task_done()
                continue

            status, code = "failed", 0
            try:
                async with session.get(current_url, timeout=15) as resp:
                    code = resp.status
                    if code == 200:
                        print(f"🔍 VISITE : {current_url} | Queue: {queue.qsize()}")
                        soup = BeautifulSoup(await resp.text(), 'html.parser')
                        new_history = history + [current_url]
                        for tag in soup.find_all('a', href=True):
                            link = urljoin(current_url, tag['href']).split('#')[0]
                            if any(link.lower().endswith(ext) for ext in EXTENSIONS):
                                asyncio.create_task(self.process_file(session, link, new_history))
                            elif urlparse(link).netloc == self.domain:
                                if not await self.is_processed(link):
                                    await queue.put((link, new_history))
                        status = "visited"
                    else:
                        status = "failed"
                        print(f"❌ ÉCHEC FINAL (Code {code}) : {url} après {attempts} essais")
            except Exception as e:
                code = 999
                status = "failed"
                print(f"⚠️ Connexion perdue sur {current_url} : {e}")
            
            await self.db.execute("INSERT OR REPLACE INTO urls VALUES (?, ?, ?, ?)", (current_url, status, code, 1))
            await self.db.commit()
            queue.task_done()

    async def run(self):
        await self.init_db()
        queue = asyncio.Queue()
        await queue.put((BASE_URL, []))
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            workers = [asyncio.create_task(self.crawl(session, queue)) for _ in range(3)]
            await queue.join()
            for w in workers: w.cancel()
        await self.db.close()

if __name__ == "__main__":
    crawler = R2Crawler()
    asyncio.run(crawler.run())
