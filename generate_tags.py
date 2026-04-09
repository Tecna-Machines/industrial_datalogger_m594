import os
import asyncio
import json
from pathlib import Path
from asyncua import Client

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

ENDPOINT = os.getenv("OPC_ENDPOINT", "opc.tcp://192.168.0.89:4840")

# Punto de partida estable (BrowseNames que vos controlás en TIA)
ROOT_PATH = ["ServerInterfaces", "M594_Datalogger", "OPC_DATOS"]

OUTFILE = Path(os.getenv("TAGS_JSON", "tags.json"))


async def get_by_path(client: Client, path: list[str]):
    node = client.nodes.objects
    for name in path:
        children = await node.get_children()
        found = None
        for ch in children:
            bn = await ch.read_browse_name()
            if bn.Name == name:
                found = ch
                break
        if not found:
            raise RuntimeError(f"No encontré '{name}' dentro de '{path}'. Quedé en {node.nodeid}")
        node = found
    return node


async def browse_recursive(node, prefix: str, out: dict, depth: int = 0, max_depth: int = 20):
    if depth > max_depth:
        return

    children = await node.get_children()
    for ch in children:
        bn = await ch.read_browse_name()
        name = bn.Name

        # Armamos una clave legible tipo: OPC-DATA.of / OPC-DATA.pt.curva
        key = f"{prefix}.{name}" if prefix else name

        # Guardamos mapping a NodeId string (ej: ns=4;i=24 o ns=3;s=PLC)
        out[key] = ch.nodeid.to_string()

        # Seguimos bajando
        await browse_recursive(ch, key, out, depth + 1, max_depth)


async def main():
    async with Client(url=ENDPOINT) as client:
        root = await get_by_path(client, ROOT_PATH)

        mapping = {}
        # prefijo base para que quede claro que viene de OPC-DATA
        await browse_recursive(root, "OPC_DATOS", mapping, 0, 50)

        OUTFILE.write_text(json.dumps(mapping, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"OK: {len(mapping)} nodos guardados en {OUTFILE.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
