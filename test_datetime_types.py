#!/usr/bin/env python3
"""
Script para probar diferentes tipos de datos de fecha/hora
"""

import asyncio
from asyncua import Client
import json

ENDPOINT = "opc.tcp://192.168.1.10:4840"

async def test_datetime_types():
    """Probar diferentes tipos de datos de fecha/hora"""
    print("=== PRUEBA DE TIPOS DE DATOS FECHA/HORA ===")
    
    try:
        async with Client(url=ENDPOINT, timeout=30) as client:
            # Cargar mapeo
            with open("tags.json", "r", encoding="utf-8") as f:
                mapeo = json.load(f)
            
            # Tags de fecha/hora a probar
            fecha_tags = []
            for tag_key in mapeo.keys():
                if "FechaYHora" in tag_key or "fecha" in tag_key.lower() or "hora" in tag_key.lower():
                    fecha_tags.append(tag_key)
            
            print(f"Tags de fecha encontrados: {len(fecha_tags)}")
            
            for tag in fecha_tags:
                nodeid = mapeo.get(tag)
                if not nodeid:
                    continue
                
                print(f"\n--- Probando: {tag} ---")
                print(f"NodeId: {nodeid}")
                
                try:
                    node = client.get_node(nodeid)
                    
                    # Obtener información del tipo
                    try:
                        data_type = await node.read_data_type()
                        print(f"Data Type: {data_type}")
                    except:
                        print("No se pudo leer Data Type")
                    
                    # Intentar leer valor
                    try:
                        valor = await asyncio.wait_for(node.read_value(), timeout=5)
                        print(f"Valor: {valor}")
                        print(f"Tipo Python: {type(valor)}")
                        
                        # Intentar procesar
                        if hasattr(valor, '__dict__'):
                            print("Atributos del objeto:")
                            for attr in dir(valor):
                                if not attr.startswith('_') and not callable(getattr(valor, attr)):
                                    try:
                                        val = getattr(valor, attr)
                                        print(f"  {attr}: {val}")
                                    except:
                                        print(f"  {attr}: <no accesible>")
                        
                    except asyncio.TimeoutError:
                        print("ERROR: Timeout")
                    except Exception as e:
                        print(f"ERROR: {e}")
                        
                except Exception as e:
                    print(f"ERROR general: {e}")
            
            # Buscar otros tags que podrían ser fechas
            print("\n--- Buscando otros tags con posibles fechas ---")
            otros_tags = []
            for tag_key in mapeo.keys():
                if any(keyword in tag_key.lower() for keyword in ['timestamp', 'time', 'date', 'dt']):
                    if "FechaYHora" not in tag_key:  # Ya los probamos
                        otros_tags.append(tag_key)
            
            print(f"Otros tags con posibles fechas: {len(otros_tags)}")
            for tag in otros_tags[:5]:  # Limitar a 5 para no saturar
                print(f"\nProbando tag alternativo: {tag}")
                nodeid = mapeo.get(tag)
                if nodeid:
                    try:
                        node = client.get_node(nodeid)
                        valor = await asyncio.wait_for(node.read_value(), timeout=3)
                        print(f"  Valor: {valor} (tipo: {type(valor)})")
                    except Exception as e:
                        print(f"  ERROR: {e}")
                        
    except Exception as e:
        print(f"Error de conexión: {e}")

if __name__ == "__main__":
    asyncio.run(test_datetime_types())
