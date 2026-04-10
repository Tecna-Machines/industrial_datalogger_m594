#!/usr/bin/env python3
"""
Script para diagnosticar problemas específicos con tags de fecha/hora
"""

import asyncio
import os
from asyncua import Client
from datetime import datetime

# Configuración
ENDPOINT = os.getenv("OPC_ENDPOINT", "opc.tcp://192.168.0.89:4840")

# Tag problemático
TAG_PROBLEMA = "OPC_DATOS.REGISTRO_EVENTOS.FALLAS.02_E4_FALLA_SENSOR_SEGURIDAD_REMACHADO.FechaYHora"

# Tags de referencia para comparar
TAGS_REFERENCIA = [
    "OPC_DATOS.GENERAL.OF",
    "OPC_DATOS.GENERAL.TURNO_ACTUAL",
    "OPC_DATOS.REGISTRO_EVENTOS.FALLAS.02_E4_FALLA_SENSOR_SEGURIDAD_REMACHADO.Categoria",
    "OPC_DATOS.REGISTRO_EVENTOS.FALLAS.02_E4_FALLA_SENSOR_SEGURIDAD_REMACHADO.ID"
]

async def diagnosticar_tag():
    """Diagnóstico detallado del tag problemático"""
    print(f"Conectando a: {ENDPOINT}")
    print(f"Tag a diagnosticar: {TAG_PROBLEMA}")
    print("=" * 60)
    
    try:
        # Configurar timeouts más largos
        client = Client(url=ENDPOINT, timeout=30)  # 30 segundos de timeout
        async with client:
            # Cargar mapeo de tags
            try:
                import json
                with open("tags.json", "r", encoding="utf-8") as f:
                    mapeo = json.load(f)
            except FileNotFoundError:
                print("ERROR: No se encuentra tags.json")
                return
            
            # 1. Verificar que el tag exista en el mapeo
            nodeid = mapeo.get(TAG_PROBLEMA)
            if not nodeid:
                print(f"ERROR: Tag {TAG_PROBLEMA} no encontrado en tags.json")
                return
            
            print(f"NodeId encontrado: {nodeid}")
            
            # 2. Obtener el nodo
            try:
                node = client.get_node(nodeid)
                print(f"Node obtenido: {node}")
            except Exception as e:
                print(f"ERROR obteniendo nodo: {e}")
                return
            
            # 3. Intentar leer el browse name
            try:
                browse_name = await node.read_browse_name()
                print(f"Browse Name: {browse_name}")
            except Exception as e:
                print(f"ERROR leyendo browse name: {e}")
            
            # 4. Intentar leer el node class
            try:
                node_class = await node.read_node_class()
                print(f"Node Class: {node_class}")
            except Exception as e:
                print(f"ERROR leyendo node class: {e}")
            
            # 5. Intentar leer el data type
            try:
                data_type = await node.read_data_type()
                print(f"Data Type: {data_type}")
            except Exception as e:
                print(f"ERROR leyendo data type: {e}")
            
            # 6. Intentar leer el valor de diferentes formas
            print("\n--- Intentos de lectura ---")
            
            # Lectura directa con timeout
            try:
                valor = await asyncio.wait_for(node.read_value(), timeout=10)
                print(f"Valor directo: {valor} (tipo: {type(valor)})")
            except asyncio.TimeoutError:
                print("ERROR lectura directa: TIMEOUT (10s)")
            except Exception as e:
                print(f"ERROR lectura directa: {e}")
            
            # Lectura con timestamp
            try:
                data_value = await asyncio.wait_for(node.read_value(), timeout=10)
                print(f"DataValue: {data_value}")
                if hasattr(data_value, 'Value'):
                    print(f"  Value: {data_value.Value}")
                if hasattr(data_value, 'SourceTimestamp'):
                    print(f"  SourceTimestamp: {data_value.SourceTimestamp}")
                if hasattr(data_value, 'ServerTimestamp'):
                    print(f"  ServerTimestamp: {data_value.ServerTimestamp}")
            except asyncio.TimeoutError:
                print("ERROR lectura DataValue: TIMEOUT (10s)")
            except Exception as e:
                print(f"ERROR lectura DataValue: {e}")
            
            # 7. Comparar con tags de referencia
            print("\n--- Comparación con tags de referencia ---")
            for tag_ref in TAGS_REFERENCIA:
                nodeid_ref = mapeo.get(tag_ref)
                if nodeid_ref:
                    try:
                        node_ref = client.get_node(nodeid_ref)
                        valor_ref = await node_ref.read_value()
                        print(f"OK {tag_ref}: {valor_ref} (tipo: {type(valor_ref)})")
                    except Exception as e:
                        print(f"ERROR {tag_ref}: {e}")
                else:
                    print(f"NO ENCONTRADO {tag_ref}")
            
            # 8. Verificar atributos del nodo
            print("\n--- Atributos del nodo ---")
            atributos = [
                "NodeId", "BrowseName", "DisplayName", "Description", 
                "WriteMask", "UserWriteMask", "IsAbstract", "Symmetric",
                "InverseName", "ContainsNoLoops", "EventNotifier"
            ]
            
            for attr in atributos:
                try:
                    if hasattr(node, f'read_{attr.lower()}'):
                        valor = await getattr(node, f'read_{attr.lower()}')()
                        print(f"{attr}: {valor}")
                except:
                    pass
            
    except Exception as e:
        print(f"ERROR general: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(diagnosticar_tag())
