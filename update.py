#!/usr/bin/env python

import requests
import httpx
import asyncio
import json
from tqdm import tqdm
import jsonlines
from datetime import datetime, timezone
import pandas as pd
import os


def listarTramites(pageSize=30):
    print("Listando trámites ...")
    tramites = []
    page = 1
    while True:
        try:
            url = (
                f"https://www.gob.bo/ws/api/portal/tramites?pagina={page}&limite={pageSize}"
            )
            response = requests.get(url)
            datos = response.json()["datos"]
            tramites.extend(
                [{k: e[k] for k in ["id", "nombre", "slug"]} for e in datos["filas"]]
            )
            print(f"{len(tramites)} de {datos['total']}")
            if len(tramites) >= datos["total"]:
                break
            else:
                page += 1
        except Exception as e:
            print(f"{e}")
    return tramites


async def getTramite(tramite_slug, client):
    """
    Descarga detalles de un trámite
    """
    url = f"https://www.gob.bo/ws/api/portal/tramites/{tramite_slug}"
    resp = await client.get(url)
    resp.raise_for_status()
    return resp.json()


async def getTramites(tramitesListado, max_concurrent=10, max_tramites=None):
    """
    Descarga detalles de un listado de trámites
    """
    tramites = []
    errores = []
    sema = asyncio.Semaphore(max_concurrent)
    subset = tramitesListado if max_tramites is None else tramitesListado[:max_tramites]
    pbar = tqdm(total=len(tramitesListado), desc="Descargando trámites")

    async def fetch_one(tramite, client):
        async with sema:
            try:
                data = await getTramite(tramite["slug"], client)
                tramites.append(data["datos"])
            except Exception as e:
                errores.append({**tramite, "error": str(e)})
            pbar.update(1)

    async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}) as client:
        await asyncio.gather(*(fetch_one(t, client) for t in subset))

    pbar.close()
    return tramites, errores


def detectarModificaciones(df1, df2, timestamp):
    FILENAME = "modificaciones.csv"
    a, b = df1.set_index("id").copy(), df2.set_index("id").copy()
    nombres = a.nombre.to_dict()
    entidades = a["entidad.nombre"].to_dict()
    cols = [c for c in a.columns if c in b.columns and c != "id"]
    idx = a.index.intersection(b.index)
    a, b = a.loc[idx, cols], b.loc[idx, cols]
    parts = []
    for c in cols:
        o, n = a[c], b[c]
        m = o.ne(n) & ~(o.isna() & n.isna())
        if m.any():
            parts.append(
                pd.DataFrame(
                    {
                        "timestamp": timestamp,
                        "id": o.index[m].values,
                        "entidad": [entidades[v] for v in o.index[m].values],
                        "nombre": [nombres[v] for v in o.index[m].values],
                        "columna": c,
                        "viejo": o[m].values,
                        "nuevo": n[m].values,
                    }
                )
            )
    if parts:
        modificaciones = pd.concat(parts, ignore_index=True)
        if os.path.exists(FILENAME):
            modificaciones = pd.concat([pd.read_csv(FILENAME), modificaciones])
        modificaciones.sort_values(["timestamp", "id", "columna"]).to_csv(
            FILENAME, index=False
        )


def detectarAdiciones(df1, df2, timestamp):
    FILENAME = "adiciones.csv"

    def formatear(df, evento, timestamp):
        n = df[["id", "entidad.nombre", "nombre"]].copy()
        n.columns = ["id", "entidad", "nombre"]
        n.insert(0, "tipo", evento)
        n.insert(0, "timestamp", timestamp)
        return n

    eventos = pd.concat(
        [
            formatear(df2[~df2["id"].isin(df1["id"])], "aparece", timestamp),
            formatear(df1[~df1["id"].isin(df2["id"])], "desaparece", timestamp),
        ]
    )

    if eventos.shape[0] > 0:

        if os.path.exists(FILENAME):
            eventos = pd.concat([pd.read_csv(FILENAME), eventos])
    
        eventos.sort_values(["timestamp", "id", "tipo"]).to_csv(FILENAME, index=False)


async def main():
    
    FILENAME = "tramites.jsonl"
    tramitesListado = listarTramites()

    print("Descargando detalles de todos los trámites ...")

    timestamp = datetime.now(timezone.utc).isoformat(timespec="minutes")
    tramites, errores_tramites = await getTramites(tramitesListado)
    if os.path.exists(FILENAME):
        tramites_df = pd.json_normalize(tramites)
        with jsonlines.open(FILENAME, "r") as f:
            tramites_previo = pd.json_normalize([line for line in f])
    
        detectarAdiciones(tramites_previo, tramites_df, timestamp)
        detectarModificaciones(tramites_previo, tramites_df, timestamp)

    tramites_sorted = sorted(tramites, key=lambda d: d["id"])
    for data, filename in zip(
        [tramites_sorted, errores_tramites], ["tramites", "errores"]
    ):
        if data:
            with open(f"{filename}.jsonl", "w", encoding="utf-8") as f:
                for entry in data:
                    json_line = json.dumps(entry, ensure_ascii=False)
                    f.write(json_line + "\n")
    print(
        f"Detalles guardados: {len(tramites_sorted)} trámites | {len(errores_tramites)} errores."
    )

if __name__ == "__main__":
    asyncio.run(main())
