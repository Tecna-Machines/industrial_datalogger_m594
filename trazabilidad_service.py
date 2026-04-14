#!/usr/bin/env python3
"""
Servicio dedicado para trazabilidad.
Script independiente que solo maneja la lectura y registro de datos de trazabilidad.
"""

import os
import json
import time
import asyncio
import logging
import signal
from datetime import datetime as dt
from typing import Dict, Any, Optional
from asyncua import Client

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

# Cargar variables de entorno desde .env si existe
try:
    from dotenv import load_dotenv
    load_dotenv()
    log.info("✅ Archivo .env cargado con python-dotenv")
except ImportError:
    # Intentar leer .env manualmente si python-dotenv no está disponible
    env_file = ".env"
    if os.path.exists(env_file):
        log.info(f"📁 Archivo .env encontrado en: {os.path.abspath(env_file)}")
        try:
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip()
            log.info("✅ Archivo .env cargado manualmente")
        except Exception as e:
            log.error(f"❌ Error leyendo .env manualmente: {e}")
    else:
        log.warning(f"⚠️  Archivo .env no encontrado en: {os.path.abspath('.')}")

# Variables de entorno
OPC_ENDPOINT = os.getenv("OPC_ENDPOINT", "opc.tcp://192.168.20.30:4840")
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "industrial_db")

# Diagnóstico: mostrar variables cargadas
log.info("🔍 Variables de entorno cargadas:")
log.info(f"  OPC_ENDPOINT: {OPC_ENDPOINT}")
log.info(f"  MYSQL_HOST: {MYSQL_HOST}")
log.info(f"  MYSQL_PORT: {MYSQL_PORT}")
log.info(f"  MYSQL_USER: {MYSQL_USER}")
log.info(f"  MYSQL_PASSWORD: {'*' * len(MYSQL_PASSWORD) if MYSQL_PASSWORD else '(vacío)'}")
log.info(f"  MYSQL_DB: {MYSQL_DB}")

# Variable global para detener el servicio
_stop = False

# Variable global para el cliente OPC UA persistente
_opc_client = None
_opc_client_lock = asyncio.Lock()

def install_signal_handlers():
    """Instala manejadores de señales para parada controlada"""
    def signal_handler(signum, frame):
        global _stop
        log.info(f"Señal de parada recibida ({signum}). Cerrando...")
        _stop = True
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

async def get_opc_client():
    """Obtiene o crea una conexión OPC UA persistente"""
    global _opc_client
    async with _opc_client_lock:
        if _opc_client is None:
            log.info("Conectando al servidor OPC UA...")
            _opc_client = Client(url=OPC_ENDPOINT)
            await _opc_client.connect()
            log.info("Conexión OPC UA establecida")
        return _opc_client

async def close_opc_client():
    """Cierra la conexión OPC UA persistente"""
    global _opc_client
    async with _opc_client_lock:
        if _opc_client is not None:
            await _opc_client.disconnect()
            _opc_client = None
            log.info("Conexión OPC UA cerrada")

def load_tags_mapping() -> Dict[str, str]:
    """Carga el mapeo de tags a NodeIds"""
    try:
        with open("tags.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        log.error("No se encontró el archivo tags.json")
        return {}
    except json.JSONDecodeError as e:
        log.error(f"Error al parsear tags.json: {e}")
        return {}

def load_mysql_connection():
    """Crea conexión a MySQL"""
    try:
        import mysql.connector
        return mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
            autocommit=True
        )
    except Exception as e:
        log.error(f"Error conectando a MySQL: {e}")
        return None

def ensure_trazabilidad_table():
    """Asegura que la tabla trazabilidad existe"""
    conn = load_mysql_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Verificar si la tabla existe
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = 'trazabilidad'
        """, (MYSQL_DB,))
        
        if cursor.fetchone()[0] == 0:
            # Crear tabla
            cursor.execute("""
                CREATE TABLE trazabilidad (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ciclo_actual INT UNSIGNED NOT NULL,
                    of INT,
                    codigo_producto_terminado VARCHAR(15),
                    codigo_polo_1 VARCHAR(23),
                    codigo_polo_2 VARCHAR(23),
                    codigo_polo_3 VARCHAR(23),
                    codigo_polo_4 VARCHAR(23),
                    codigo_inspecciones INT,
                    turno INT,
                    fecha_hora DATETIME NOT NULL,
                    INDEX idx_ciclo_of (ciclo_actual, of),
                    INDEX idx_of (of)
                )
            """)
            log.info("Tabla trazabilidad creada")
        else:
            log.debug("Tabla trazabilidad ya existe")
        
        cursor.close()
        return True
        
    except Exception as e:
        log.error(f"Error verificando/creando tabla trazabilidad: {e}")
        return False
    finally:
        conn.close()

def insertar_trazabilidad(datos: Dict[str, Any]) -> bool:
    """Inserta datos de trazabilidad en la base de datos"""
    conn = load_mysql_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        sql = """
        INSERT INTO trazabilidad (ciclo_actual, `of`, codigo_producto_terminado, 
                                codigo_polo_1, codigo_polo_2, codigo_polo_3, codigo_polo_4,
                                codigo_inspecciones, turno, fecha_hora)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        values = (
            datos.get("ciclo_actual"),
            datos.get("of"),
            datos.get("codigo_producto_terminado"),
            datos.get("codigo_polo_1"),
            datos.get("codigo_polo_2"),
            datos.get("codigo_polo_3"),
            datos.get("codigo_polo_4"),
            datos.get("codigo_inspecciones"),
            datos.get("turno"),
            datos.get("fecha_hora")
        )
        
        cursor.execute(sql, values)
        log.debug(f"Insertado en trazabilidad: {datos}")
        return True
        
    except Exception as e:
        log.error(f"Error insertando en trazabilidad: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

async def leer_tags_trazabilidad(tags_mapping: Dict[str, str]) -> Dict[str, Any]:
    """Lee solo los tags necesarios para trazabilidad"""
    tags_trazabilidad = [
        "OPC_DATOS.TRAZABILIDAD.CICLO_ACTUAL",
        "OPC_DATOS.TRAZABILIDAD.CODIGO_PRODUCTO_TERMINADO",
        "OPC_DATOS.TRAZABILIDAD.CODIGO_POLO_1",
        "OPC_DATOS.TRAZABILIDAD.CODIGO_POLO_2",
        "OPC_DATOS.TRAZABILIDAD.CODIGO_POLO_3",
        "OPC_DATOS.TRAZABILIDAD.CODIGO_POLO_4",
        "OPC_DATOS.TRAZABILIDAD.CODIGO_INSPECCIONES",
        "OPC_DATOS.GENERAL.TURNO_ACTUAL",
        "OPC_DATOS.GENERAL.OF"
    ]
    
    valores = {}
    client = await get_opc_client()
    
    for tag_key in tags_trazabilidad:
        try:
            nodeid = tags_mapping.get(tag_key)
            if not nodeid:
                log.warning(f"No se encontró NodeId para tag {tag_key}")
                valores[tag_key] = None
                continue
            
            node = client.get_node(nodeid)
            valor = await node.read_value()
            
            # Procesar valor
            if hasattr(valor, '__class__'):
                tipo_str = str(type(valor))
                if 'DInt' in tipo_str or 'UDInt' in tipo_str or 'UInt' in tipo_str:
                    valor = int(valor) if valor is not None else 0
            
            valores[tag_key] = valor
            log.debug(f"Leído {tag_key}: {valor}")
            
        except Exception as e:
            log.warning(f"No se pudo leer tag {tag_key}: {e}")
            valores[tag_key] = None
    
    return valores

class TrazabilidadStateTracker:
    """Manejo de estado para trazabilidad"""
    
    def __init__(self):
        self.estado = {}
    
    def get_estado(self, clave: str, subclave: str = None):
        """Obtiene un valor del estado"""
        if subclave:
            return self.estado.get(clave, {}).get(subclave)
        return self.estado.get(clave)
    
    def set_estado(self, clave: str, subclave: str, valor: Any):
        """Establece un valor en el estado"""
        if clave not in self.estado:
            self.estado[clave] = {}
        self.estado[clave][subclave] = valor

def procesar_trazabilidad(valores: Dict[str, Any], state_tracker: TrazabilidadStateTracker) -> int:
    """Procesa trazabilidad con buffer de ciclos"""
    ciclo_actual = valores.get("OPC_DATOS.TRAZABILIDAD.CICLO_ACTUAL")
    of_actual = valores.get("OPC_DATOS.GENERAL.OF")
    
    log.info(f"Traza - Ciclo: {ciclo_actual}, OF: {of_actual}")
    
    if ciclo_actual is None or of_actual is None:
        log.warning(f"Traza - Datos incompletos: ciclo={ciclo_actual}, of={of_actual}")
        return 0
    
    # Obtener último estado
    ultimo_ciclo = state_tracker.get_estado("trazabilidad", "ciclo_actual")
    ultima_of = state_tracker.get_estado("trazabilidad", "of")
    
    log.info(f"Traza - Último ciclo: {ultimo_ciclo}, Última OF: {ultima_of}")
    
    debe_registrar = False
    
    if ultimo_ciclo is None or ultima_of is None:
        debe_registrar = True
        log.info(f"Traza - Primera vez, se registrará")
    else:
        # Verificar si cambió la OF
        if ultima_of != of_actual:
            log.info(f"Traza - OF cambió ({ultima_of} -> {of_actual}), reseteando tracking")
            state_tracker.set_estado("trazabilidad", "ciclo_actual", None)
            state_tracker.set_estado("trazabilidad", "of", None)
            debe_registrar = True
            log.info(f"Traza - Nueva OF, se registrará primer ciclo")
        else:
            # Condición: misma OF y ciclo mayor
            if (ultima_of == of_actual and 
                isinstance(ciclo_actual, (int, float)) and 
                isinstance(ultimo_ciclo, (int, float)) and
                ciclo_actual > ultimo_ciclo):
                debe_registrar = True
                log.info(f"Traza - Ciclo mayor ({ciclo_actual} > {ultimo_ciclo}), se registrará")
            else:
                log.info(f"Traza - No se registra: misma OF={ultima_of == of_actual}, ciclo mayor={ciclo_actual > ultimo_ciclo if isinstance(ciclo_actual, (int, float)) and isinstance(ultimo_ciclo, (int, float)) else 'N/A'}")
    
    if debe_registrar:
        # Implementar buffer de ciclos
        ultimo_ciclo_registrado = state_tracker.get_estado("trazabilidad", "ciclo_actual")
        ciclos_perdidos = []
        
        if ultimo_ciclo_registrado is not None:
            for ciclo_perdido in range(int(ultimo_ciclo_registrado) + 1, int(ciclo_actual)):
                ciclos_perdidos.append(ciclo_perdido)
        
        registros_insertados = 0
        
        # Registrar ciclos perdidos
        for ciclo in ciclos_perdidos:
            datos = {
                "ciclo_actual": ciclo,
                "of": str(of_actual) if of_actual is not None else None,
                "codigo_producto_terminado": valores.get("OPC_DATOS.TRAZABILIDAD.CODIGO_PRODUCTO_TERMINADO"),
                "codigo_polo_1": valores.get("OPC_DATOS.TRAZABILIDAD.CODIGO_POLO_1"),
                "codigo_polo_2": valores.get("OPC_DATOS.TRAZABILIDAD.CODIGO_POLO_2"),
                "codigo_polo_3": valores.get("OPC_DATOS.TRAZABILIDAD.CODIGO_POLO_3"),
                "codigo_polo_4": valores.get("OPC_DATOS.TRAZABILIDAD.CODIGO_POLO_4"),
                "codigo_inspecciones": valores.get("OPC_DATOS.TRAZABILIDAD.CODIGO_INSPECCIONES"),
                "turno": valores.get("OPC_DATOS.GENERAL.TURNO_ACTUAL"),
                "fecha_hora": dt.now()
            }
            
            if insertar_trazabilidad(datos):
                log.info(f"Traza - Registrado ciclo perdido {ciclo} para OF {of_actual}")
                registros_insertados += 1
            else:
                log.error(f"Traza - Error insertando ciclo perdido {ciclo}")
        
        # Registrar ciclo actual
        datos = {
            "ciclo_actual": int(ciclo_actual) if ciclo_actual is not None else 0,
            "of": str(of_actual) if of_actual is not None else None,
            "codigo_producto_terminado": valores.get("OPC_DATOS.TRAZABILIDAD.CODIGO_PRODUCTO_TERMINADO"),
            "codigo_polo_1": valores.get("OPC_DATOS.TRAZABILIDAD.CODIGO_POLO_1"),
            "codigo_polo_2": valores.get("OPC_DATOS.TRAZABILIDAD.CODIGO_POLO_2"),
            "codigo_polo_3": valores.get("OPC_DATOS.TRAZABILIDAD.CODIGO_POLO_3"),
            "codigo_polo_4": valores.get("OPC_DATOS.TRAZABILIDAD.CODIGO_POLO_4"),
            "codigo_inspecciones": valores.get("OPC_DATOS.TRAZABILIDAD.CODIGO_INSPECCIONES"),
            "turno": valores.get("OPC_DATOS.GENERAL.TURNO_ACTUAL"),
            "fecha_hora": dt.now()
        }
        
        if insertar_trazabilidad(datos):
            # Actualizar estado
            state_tracker.set_estado("trazabilidad", "ciclo_actual", ciclo_actual)
            state_tracker.set_estado("trazabilidad", "of", of_actual)
            
            registros_insertados += 1
            
            if ciclos_perdidos:
                log.info(f"Traza - Registrado ciclo actual {ciclo_actual} + {len(ciclos_perdidos)} ciclos perdidos para OF {of_actual}")
            else:
                log.info(f"Traza - Registrado ciclo {ciclo_actual} para OF {of_actual}")
            
            return registros_insertados
        else:
            log.error(f"Traza - Error insertando ciclo {ciclo_actual}")
            return 0
    
    return 0

async def run_trazabilidad_service():
    """Servicio principal de trazabilidad"""
    log.info("Iniciando servicio de trazabilidad...")
    log.info(f"OPC UA: {OPC_ENDPOINT}")
    log.info(f"MySQL: {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}")
    
    # Verificar tabla
    if not ensure_trazabilidad_table():
        log.error("No se pudo verificar/crear tabla trazabilidad")
        return
    
    # Cargar mapeo de tags
    tags_mapping = load_tags_mapping()
    if not tags_mapping:
        log.error("No se pudo cargar mapeo de tags")
        return
    
    # Estado para control
    state_tracker = TrazabilidadStateTracker()
    
    # Estadísticas
    ciclos_totales = 0
    registros_totales = 0
    ultimo_log_stats = time.time()
    
    log.info("Iniciando ciclo de trazabilidad...")
    
    try:
        while not _stop:
            ciclo_inicio = time.time()
            
            # Leer tags de trazabilidad
            valores = await leer_tags_trazabilidad(tags_mapping)
            
            # Procesar trazabilidad
            registros_insertados = procesar_trazabilidad(valores, state_tracker)
            registros_totales += registros_insertados
            
            ciclos_totales += 1
            
            # Estadísticas cada 60 segundos
            if time.time() - ultimo_log_stats >= 60:
                log.info(f"Traza - Estadísticas: {ciclos_totales} ciclos, {registros_totales} registros totales")
                ultimo_log_stats = time.time()
            
            # Esperar para próximo ciclo (1 segundo)
            ciclo_tiempo = time.time() - ciclo_inicio
            espera = max(0.5, 1.0 - ciclo_tiempo)
            if espera > 0:
                await asyncio.sleep(espera)
    
    finally:
        await close_opc_client()
    
    log.info("Servicio de trazabilidad detenido")

if __name__ == "__main__":
    install_signal_handlers()
    try:
        asyncio.run(run_trazabilidad_service())
    except KeyboardInterrupt:
        log.info("Interrumpido por usuario")
    except Exception as e:
        log.error(f"Error fatal: {e}")
        log.exception("Detalles:")
