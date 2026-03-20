import asyncio
import aiosqlite
import json
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, HTMLResponse
from crawler R2Crawler, DB_NAME

app = FastAPI(title="Crawler Stats Dashboard")
DB_NAME = "crawler_state.db"

async def get_db_stats():
    """Interroge SQLite pour extraire les compteurs actuels."""
    try:
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT status, COUNT(*) FROM urls GROUP BY status") as cursor:
                rows = await cursor.fetchall()
                # Transforme [(status, count), ...] en dict {status: count}
                stats = {row[0]: row[1] for row in rows}
                
                # Ajoute le détail des erreurs (optionnel)
                async with db.execute("SELECT http_code, COUNT(*) FROM urls WHERE status='failed' GROUP BY http_code") as err_cursor:
                    errors = await err_cursor.fetchall()
                    stats['errors_detail'] = {f"code_{r[0]}": r[1] for r in errors}
                
                return stats
    except Exception as e:
        return {"error": str(e)}

async def stats_generator(request: Request):
    """Générateur SSE qui envoie les stats en continu."""
    while True:
        # Si l'utilisateur ferme l'onglet, on arrête de boucler
        if await request.is_disconnected():
            break

        stats = await get_db_stats()
        # Format SSE : "data: <json>\n\n"
        yield f"data: {json.dumps(stats)}\n\n"
        
        await asyncio.sleep(2) # Mise à jour toutes les 2 secondes


@app.get("/", response_class=HTMLResponse)
async def index():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/stats/stream")
async def stream_stats(request: Request):
    return StreamingResponse(stats_generator(request), media_type="text/event-stream")

@app.on_event("startup")
async def startup_event():
    """Lance le crawler dès que FastAPI démarre."""
    crawler = R2Crawler()
    asyncio.create_task(crawler.run())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
