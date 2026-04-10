#!/usr/bin/env python3
"""
Script para probar lectura de componentes DTL individuales
"""

import asyncio
from asyncua import Client
import json

ENDPOINT = "opc.tcp://192.168.1.10:4840"

async def test_dtl_components():
    """Probar lectura de componentes DTL individuales"""
    print("=== PRUEBA DE COMPONENTES DTL ===")
    
    try:
        async with Client(url=ENDPOINT, timeout=30) as client:
            # Cargar mapeo
            with open("tags.json", "r", encoding="utf-8") as f:
                mapeo = json.load(f)
            
            # Evento de prueba
            evento_base = "OPC_DATOS.REGISTRO_EVENTOS.FALLAS.02_E4_FALLA_SENSOR_SEGURIDAD_REMACHADO"
            
            # Componentes DTL a probar
            componentes = ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE"]
            
            print(f"\nProbando evento: {evento_base}")
            print(f"Componentes DTL: {', '.join(componentes)}")
            
            valores_leidos = {}
            
            for componente in componentes:
                tag_path = f"{evento_base}.FechaYHora.{componente}"
                nodeid = mapeo.get(tag_path)
                
                if nodeid:
                    print(f"\n--- {componente} ---")
                    print(f"Tag: {tag_path}")
                    print(f"NodeId: {nodeid}")
                    
                    try:
                        node = client.get_node(nodeid)
                        valor = await asyncio.wait_for(node.read_value(), timeout=5)
                        
                        print(f"Valor: {valor}")
                        print(f"Tipo: {type(valor)}")
                        
                        valores_leidos[componente] = valor
                        
                    except Exception as e:
                        print(f"ERROR: {e}")
                        valores_leidos[componente] = None
                else:
                    print(f"\n--- {componente} ---")
                    print(f"Tag: {tag_path}")
                    print(f"ERROR: No encontrado en mapeo")
                    valores_leidos[componente] = None
            
            # Reconstruir fecha si tenemos todos los componentes
            print(f"\n=== RECONSTRUCCIÓN DE FECHA ===")
            if all(v is not None for v in valores_leidos.values()):
                try:
                    from datetime import datetime as dt
                    
                    year = int(valores_leidos["YEAR"]) if valores_leidos["YEAR"] != 0 else 2024
                    month = int(valores_leidos["MONTH"]) if valores_leidos["MONTH"] != 0 else 1
                    day = int(valores_leidos["DAY"]) if valores_leidos["DAY"] != 0 else 1
                    hour = int(valores_leidos["HOUR"])
                    minute = int(valores_leidos["MINUTE"])
                    
                    fecha_dt = dt(year, month, day, hour, minute, 0)
                    print(f"Fecha reconstruida: {fecha_dt}")
                    print(f"Formato MySQL: {fecha_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                    
                except Exception as e:
                    print(f"ERROR reconstruyendo fecha: {e}")
            else:
                print("No se pueden reconstruir fecha - faltan componentes")
                for comp, val in valores_leidos.items():
                    status = "OK" if val is not None else "ERROR"
                    print(f"  {comp}: {status}")
            
            # Probar también otros eventos
            print(f"\n=== OTROS EVENTOS DISPONIBLES ===")
            eventos_encontrados = set()
            
            for tag_key in mapeo.keys():
                if "FechaYHora.YEAR" in tag_key:
                    # Extraer nombre del evento
                    partes = tag_key.split(".")
                    if len(partes) >= 5:
                        evento = ".".join(partes[:5])  # OPC_DATOS.REGISTRO_EVENTOS.FALLAS.NOMBRE_EVENTO
                        eventos_encontrados.add(evento)
            
            print(f"Eventos con componentes DTL: {len(eventos_encontrados)}")
            for evento in sorted(eventos_encontrados)[:5]:  # Mostrar solo los primeros 5
                print(f"  {evento}")
            
    except Exception as e:
        print(f"Error de conexión: {e}")

if __name__ == "__main__":
    asyncio.run(test_dtl_components())
