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
    """
    Lista todos los trámites disponibles.
    """

    print("Listando trámites ...")
    tramites = []
    page = 1
    while True:
        try:
            url = f"https://www.gob.bo/ws/api/portal/tramites?pagina={page}&limite={pageSize}"
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
    Descarga datos de un trámite.
    """
    url = f"https://www.gob.bo/ws/api/portal/tramites/{tramite_slug}"
    resp = await client.get(url)
    resp.raise_for_status()
    return resp.json()


async def getTramites(tramitesListado, max_concurrent=10, max_tramites=None):
    """
    Descarga asíncrona de datos de trámites en un listado.
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
    """
    Detecta trámites que cambian entre dos corridas
    consecutivas df1 y df2. Construye y guarda una
    bitácora de estos trámites más una estampa de tiempo.
    """

    FILENAME = "modificaciones.csv"

    # Alinear trámites
    _df1, _df2 = df1.set_index("id").copy(), df2.set_index("id").copy()
    nombres = _df1.nombre.to_dict()
    entidades = _df1["entidad.nombre"].to_dict()
    cols = [c for c in _df1.columns if c in _df2.columns and c != "id"]
    idx = _df1.index.intersection(_df2.index)
    _df1, _df2 = _df1.loc[idx, cols], _df2.loc[idx, cols]

    # Comparar cada columna e identificar cambios
    parts = []
    for col in cols:
        old, new = _df1[col], _df2[col]
        modified = old.ne(new) & ~(old.isna() & new.isna())
        if modified.any():
            parts.append(
                pd.DataFrame(
                    {
                        "timestamp": timestamp,
                        "id": old.index[modified].values,
                        "entidad": [entidades[v] for v in old.index[modified].values],
                        "nombre": [nombres[v] for v in old.index[modified].values],
                        "columna": col,
                        "viejo": old[modified].values,
                        "nuevo": new[modified].values,
                    }
                )
            )

    # Guardar cambios
    print(f"{len(parts)} modificaciones")
    if len(parts) > 0:
        modificaciones = pd.concat(parts, ignore_index=True)
        if os.path.exists(FILENAME):
            modificaciones = pd.concat([pd.read_csv(FILENAME), modificaciones])
        modificaciones.sort_values(["timestamp", "id", "columna"]).to_csv(
            FILENAME, index=False
        )


def detectarAdiciones(df1, df2, timestamp):
    """
    Detecta trámites que aparecen o desaparecen
    entre dos corridas consecutivas df1 y df2.
    Construye y guarda una bitácora de estos trámites
    más una estampa de tiempo.
    """

    FILENAME = "adiciones.csv"

    # El formato de la bitácora
    def formatear(df, evento, timestamp):
        n = df[["id", "entidad.nombre", "nombre"]].copy()
        n.columns = ["id", "entidad", "nombre"]
        n.insert(0, "tipo", evento)
        n.insert(0, "timestamp", timestamp)
        return n

    # Detectar trámites que aparecen o desaparecen
    eventos = pd.concat(
        [
            formatear(df2[~df2["id"].isin(df1["id"])], "aparece", timestamp),
            formatear(df1[~df1["id"].isin(df2["id"])], "desaparece", timestamp),
        ]
    )

    # Guardar registros
    print(f"{len(eventos.shape[0])} trámites que aparecen o desaparecen")
    if eventos.shape[0] > 0:
        if os.path.exists(FILENAME):
            eventos = pd.concat([pd.read_csv(FILENAME), eventos])

        eventos.sort_values(["timestamp", "id", "tipo"]).to_csv(FILENAME, index=False)


async def main():
    """
    Lista todos los trámites disponibles y descarga
    datos para cada uno en una serie de reintentos.
    Luego guarda todos estos datos más posibles errores
    junto a bitácoras de trámites que aparecen, desaparecen
    o son modificados entre corridas consecutivas.
    """

    FILENAME = "tramites.jsonl"
    RETRIES = 5

    # Listar tramites
    tramitesListado = listarTramites()
    print(f"{len(tramitesListado)} tramites listados")

    # Estampa de tiempo
    timestamp = datetime.now(timezone.utc).isoformat(timespec="minutes")

    # Descargar datos de cada trámite y reintentar errores
    tramites, errores_tramites = await getTramites(tramitesListado)
    print(f"{len(tramites)} registros, {len(errores_tramites)} errores")
    while errores_tramites or RETRIES == 0:
        print("Reintentando errores ...")
        tramites_retried, errores_tramites = await getTramites(errores_tramites)
        tramites.extend(tramites_retried)
        RETRIES -= 1
        print(f"{len(tramites)} registros, {len(errores_tramites)} errores")

    # Consolidar con datos recogidos previamente
    if os.path.exists(FILENAME):
        tramites_df = pd.json_normalize(tramites)
        with jsonlines.open(FILENAME, "r") as f:
            tramites_previos = pd.json_normalize([line for line in f])

        detectarAdiciones(tramites_previos, tramites_df, timestamp)
        detectarModificaciones(tramites_previos, tramites_df, timestamp)

    # Guardar trámites y errores
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
        f"Datos guardados: {len(tramites_sorted)} trámites | {len(errores_tramites)} errores."
    )


if __name__ == "__main__":
    asyncio.run(main())
