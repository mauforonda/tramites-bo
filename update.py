#!/usr/bin/env python

import requests
import httpx
import asyncio
import json
from tqdm import tqdm


def getEntidades():
    """
    Descarga una lista de entidades
    """
    url = "https://tramites.agetic.gob.bo/ws2/api/v1/pu/entidad"
    response = requests.get(url)
    return [
        {k: e[k] for k in ["id_entidad", "denominacion", "sigla"]}
        for e in response.json()["datos"]
    ]


def getEntidad(entidad, pageSize=10):
    """
    Descarga una lista de trámites para una entidad
    """
    tramites = []
    page = 1
    total = 0
    while True:
        url = f"https://tramites.agetic.gob.bo/ws2/api/v1/pu/busqueda?entidad={entidad}&limit={pageSize}&page={page}"
        response = requests.get(url)
        r = response.json()["datos"]
        total = r["resultadoResumen"]["count"]
        tramites.extend(r["tramites"])
        if len(tramites) >= total:
            break
        else:
            page += 1
    return tramites


async def getTramite(tramite_id, client):
    """
    Descarga detalles de un trámite
    """
    url = f"https://tramites.agetic.gob.bo/ws2/api/v1/pu/tramite/{tramite_id}"
    resp = await client.get(url)
    resp.raise_for_status()
    return resp.json()


async def getTramites(tramitesListado, max_concurrent=10):
    """
    Descarga detalles de un listado de trámites
    """
    tramites = []
    errores = []
    sema = asyncio.Semaphore(max_concurrent)
    pbar = tqdm(total=len(tramitesListado), desc="Descargando trámites")

    async def fetch_one(tramite, client):
        async with sema:
            try:
                data = await getTramite(tramite["id"], client)
                tramites.append(data["datos"])
            except Exception as e:
                errores.append({**tramite, "error": str(e)})
            pbar.update(1)

    async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}) as client:
        await asyncio.gather(*(fetch_one(t, client) for t in tramitesListado))

    pbar.close()
    return tramites, errores


async def main():
    print("Revisando el listado de entidades ...")
    entidades = getEntidades()

    print("Descargando el listado de trámites ...")
    tramitesListado = []
    errores = []

    for i, entidad in enumerate(entidades):
        try:
            tramitesEntidad = getEntidad(entidad["id_entidad"])
            tramitesListado.extend(tramitesEntidad)
        except Exception as e:
            errores.append({**entidad, **{"error": e}})

    print(
        f"Existen {len(tramitesListado)} trámites en {len(entidades)} | {len(errores)} errores"
    )

    print("Descargando detalles de todos los trámites ...")

    tramites, errores_tramites = await getTramites(tramitesListado)
    tramites_sorted = sorted(tramites, key=lambda d: d["id_tramite"])
    for data, filename in zip(
        [tramites_sorted, errores_tramites], ["tramites", "errores"]
    ):
        with open(f"{filename}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Detalles guardados: {len(tramites_sorted)} trámites | {len(errores_tramites)} errores.")

if __name__ == "__main__":
    asyncio.run(main())
