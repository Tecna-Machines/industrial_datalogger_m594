#!/usr/bin/env python3
"""
Servicio dedicado para estadísticas.
Script independiente que solo maneja la lectura y registro de datos estadísticos.
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
    log.info("Archivos .env cargado con python-dotenv")
except ImportError:
    # Intentar leer .env manualmente si python-dotenv no está disponible
    env_file = ".env"
    if os.path.exists(env_file):
        log.info(f"Archivo .env encontrado en: {os.path.abspath(env_file)}")
        try:
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip()
            log.info("Archivo .env cargado manualmente")
        except Exception as e:
            log.error(f"Error leyendo .env manualmente: {e}")
    else:
        log.warning(f"Archivo .env no encontrado en: {os.path.abspath('.')}")

# Variables de entorno
OPC_ENDPOINT = os.getenv("OPC_ENDPOINT", "opc.tcp://192.168.20.30:4840")
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "industrial_db")

# Diagnóstico: mostrar variables cargadas
log.info("Variables de entorno cargadas:")
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
        if not _stop:  # Solo mostrar mensaje la primera vez
            log.info(f"Señal de parada recibida ({signum}). Cerrando...")
            _stop = True
    
    # Usar signal.SIG_DFL para restaurar comportamiento por defecto después de manejar
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

async def get_opc_client():
    """Obtiene o crea una conexión OPC UA persistente con reconexión automática"""
    global _opc_client
    async with _opc_client_lock:
        # Verificar si el cliente existe y está conectado
        if _opc_client is not None:
            try:
                # Intentar una operación simple para verificar conexión
                await _opc_client.get_namespace_array()
                return _opc_client
            except Exception:
                log.warning("Conexión OPC UA perdida, intentando reconectar...")
                try:
                    await _opc_client.disconnect()
                except:
                    pass
                _opc_client = None
        
        # Crear nueva conexión
        max_retries = 3
        for attempt in range(max_retries):
            try:
                log.info(f"Conectando al servidor OPC UA (intento {attempt + 1}/{max_retries})...")
                _opc_client = Client(url=OPC_ENDPOINT)
                await _opc_client.connect()
                log.info("Conexión OPC UA establecida")
                return _opc_client
            except Exception as e:
                log.error(f"Error conectando OPC UA (intento {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(5)  # Esperar 5 segundos antes de reintentar
                else:
                    log.error("No se pudo establecer conexión OPC UA después de varios intentos")
                    _opc_client = None
                    return None

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
    """Crea conexión a MySQL con reconexión automática"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            import mysql.connector
            conn = mysql.connector.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DB,
                autocommit=True
            )
            
            # Verificar que la conexión funciona con ping
            if conn.is_connected():
                log.info("Conexión MySQL establecida")
                return conn
            else:
                log.warning(f"Conexión MySQL no estable (intento {attempt + 1})")
                conn.close()
                
        except Exception as e:
            log.error(f"Error conectando MySQL (intento {attempt + 1}): {e}")
            
        if attempt < max_retries - 1:
            log.info(f"Esperando 5 segundos antes de reintentar conexión MySQL...")
            time.sleep(5)
        else:
            log.error("No se pudo establecer conexión MySQL después de varios intentos")
            return None

def verify_mysql_connection(conn):
    """Verifica si la conexión MySQL sigue activa y la repara si es necesario"""
    try:
        if conn is None or not conn.is_connected():
            log.warning("Conexión MySQL perdida, intentando reconectar...")
            return load_mysql_connection()
        return conn
    except Exception as e:
        log.error(f"Error verificando conexión MySQL: {e}")
        return load_mysql_connection()

def ensure_estadisticas_table():
    """Asegura que la tabla estadisticas existe"""
    conn = load_mysql_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Verificar si la tabla existe
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = 'estadisticas'
        """, (MYSQL_DB,))
        
        if cursor.fetchone()[0] == 0:
            # Crear tabla
            cursor.execute("""
                CREATE TABLE estadisticas (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    buenas_totales INT,
                    malas_totales INT,
                    produccion_faltante INT UNSIGNED,
                    malas_por_e1_qr INT,
                    malas_por_e1_inspeccioninicial INT,
                    malas_por_e3_verificacionintegral INT,
                    malas_por_e6_remacheysoldadura INT,
                    malas_por_e10_presenciaacoples INT,
                    malas_por_e11_polonoutilizado INT,
                    malas_por_e12_testalturatermica INT,
                    malas_por_e14_inspeccionfinal INT,
                    `of` INT,
                    turno INT,
                    fecha_hora DATETIME NOT NULL,
                    INDEX idx_fecha_hora (fecha_hora),
                    INDEX idx_of (`of`)
                )
            """)
            log.info("Tabla estadisticas creada")
        else:
            log.debug("Tabla estadisticas ya existe")
        
        cursor.close()
        return True
        
    except Exception as e:
        log.error(f"Error verificando/creando tabla estadisticas: {e}")
        return False
    finally:
        conn.close()

def insertar_estadisticas(datos: Dict[str, Any]) -> bool:
    """Inserta datos de estadísticas en la base de datos con reconexión automática"""
    if not datos:
        return False
    
    # Obtener conexión con verificación automática
    conn = load_mysql_connection()
    if not conn:
        return False
    
    try:
        # Verificar conexión antes de usar
        conn = verify_mysql_connection(conn)
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        sql = """
        INSERT INTO estadisticas (buenas_totales, malas_totales, produccion_faltante,
                                  malas_por_e1_qr, malas_por_e1_inspeccioninicial, malas_por_e3_verificacionintegral,
                                  malas_por_e6_remacheysoldadura, malas_por_e10_presenciaacoples, malas_por_e11_polonoutilizado,
                                  malas_por_e12_testalturatermica, malas_por_e14_inspeccionfinal, `of`, turno, fecha_hora)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        values = (
            datos.get("buenas_totales"),
            datos.get("malas_totales"),
            datos.get("produccion_faltante"),
            datos.get("malas_por_e1_qr"),
            datos.get("malas_por_e1_inspeccioninicial"),
            datos.get("malas_por_e3_verificacionintegral"),
            datos.get("malas_por_e6_remacheysoldadura"),
            datos.get("malas_por_e10_presenciaacoples"),
            datos.get("malas_por_e11_polonoutilizado"),
            datos.get("malas_por_e12_testalturatermica"),
            datos.get("malas_por_e14_inspeccionfinal"),
            datos.get("of"),
            datos.get("turno"),
            datos.get("fecha_hora")
        )
        
        cursor.execute(sql, values)
        log.debug(f"Insertado en estadisticas: {datos}")
        return True
        
    except Exception as e:
        log.error(f"Error insertando en estadisticas: {e}")
        # Intentar reconectar y reintentar una vez
        try:
            conn = verify_mysql_connection(None)
            if conn:
                cursor = conn.cursor()
                cursor.execute(sql, values)
                log.info(f"Reintentado insertado en estadisticas: {datos}")
                return True
        except Exception as retry_e:
            log.error(f"Error en reintento insertando estadisticas: {retry_e}")
        return False
    finally:
        try:
            cursor.close()
        except:
            pass
        try:
            conn.close()
        except:
            pass

async def leer_tags_estadisticas(tags_mapping: Dict[str, str]) -> Dict[str, Any]:
    """Lee solo los tags necesarios para estadísticas"""
    tags_estadisticas = [
        "OPC_DATOS.ESTADISTICAS.BuenasTotales",
        "OPC_DATOS.ESTADISTICAS.MalasTotales",
        "OPC_DATOS.ESTADISTICAS.ProduccionFaltante",
        "OPC_DATOS.ESTADISTICAS.MalasPor_E1_QR",
        "OPC_DATOS.ESTADISTICAS.MalasPor_E1_InspeccionInicial",
        "OPC_DATOS.ESTADISTICAS.MalasPor_E3_VerificacionIntegral",
        "OPC_DATOS.ESTADISTICAS.MalasPor_E6_RemacheYSoldadura",
        "OPC_DATOS.ESTADISTICAS.MalasPor_E10_PresenciaAcoples",
        "OPC_DATOS.ESTADISTICAS.MalasPor_E11_PoloNoUtilizado",
        "OPC_DATOS.ESTADISTICAS.MalasPor_E12_TestAlturaTermica",
        "OPC_DATOS.ESTADISTICAS.MalasPor_E14_InspeccionFinal",
        "OPC_DATOS.GENERAL.OF",
        "OPC_DATOS.GENERAL.TURNO_ACTUAL"
    ]
    
    valores = {}
    client = await get_opc_client()
    
    for tag_key in tags_estadisticas:
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

def procesar_estadisticas(valores: Dict[str, Any]) -> Dict[str, Any]:
    """Procesa datos de estadísticas para inserción"""
    of_actual = valores.get("OPC_DATOS.GENERAL.OF")
    
    log.info(f"Estadísticas - OF: {of_actual}")
    
    if of_actual is None:
        log.warning(f"Estadísticas - OF es None, no se registrará")
        return None
    
    # Mapear tags a nombres de columnas
    datos = {
        "buenas_totales": valores.get("OPC_DATOS.ESTADISTICAS.BuenasTotales"),
        "malas_totales": valores.get("OPC_DATOS.ESTADISTICAS.MalasTotales"),
        "produccion_faltante": valores.get("OPC_DATOS.ESTADISTICAS.ProduccionFaltante"),
        "malas_por_e1_qr": valores.get("OPC_DATOS.ESTADISTICAS.MalasPor_E1_QR"),
        "malas_por_e1_inspeccioninicial": valores.get("OPC_DATOS.ESTADISTICAS.MalasPor_E1_InspeccionInicial"),
        "malas_por_e3_verificacionintegral": valores.get("OPC_DATOS.ESTADISTICAS.MalasPor_E3_VerificacionIntegral"),
        "malas_por_e6_remacheysoldadura": valores.get("OPC_DATOS.ESTADISTICAS.MalasPor_E6_RemacheYSoldadura"),
        "malas_por_e10_presenciaacoples": valores.get("OPC_DATOS.ESTADISTICAS.MalasPor_E10_PresenciaAcoples"),
        "malas_por_e11_polonoutilizado": valores.get("OPC_DATOS.ESTADISTICAS.MalasPor_E11_PoloNoUtilizado"),
        "malas_por_e12_testalturatermica": valores.get("OPC_DATOS.ESTADISTICAS.MalasPor_E12_TestAlturaTermica"),
        "malas_por_e14_inspeccionfinal": valores.get("OPC_DATOS.ESTADISTICAS.MalasPor_E14_InspeccionFinal"),
        "of": str(of_actual) if of_actual is not None else None,
        "turno": valores.get("OPC_DATOS.GENERAL.TURNO_ACTUAL"),
        "fecha_hora": dt.now()
    }
    
    log.info(f"Estadísticas - Registrando para OF {of_actual}")
    log.debug(f"Estadísticas - Datos: {datos}")
    
    return datos

async def run_estadisticas_service():
    """Servicio principal de estadísticas"""
    log.info("Iniciando servicio de estadísticas...")
    log.info(f"OPC UA: {OPC_ENDPOINT}")
    log.info(f"MySQL: {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}")
    
    # Verificar tabla
    if not ensure_estadisticas_table():
        log.error("No se pudo verificar/crear tabla estadisticas")
        return
    
    # Cargar mapeo de tags
    tags_mapping = load_tags_mapping()
    if not tags_mapping:
        log.error("No se pudo cargar mapeo de tags")
        return
    
    # Estadísticas
    ciclos_totales = 0
    registros_totales = 0
    ultimo_log_stats = time.time()
    
    log.info("Iniciando ciclo de estadísticas (cada 3600 segundos)...")
    
    try:
        while not _stop:
            ciclo_inicio = time.time()
            
            # Leer tags de estadísticas
            valores = await leer_tags_estadisticas(tags_mapping)
            
            # Procesar estadísticas
            datos = procesar_estadisticas(valores)
            
            if datos:
                if insertar_estadisticas(datos):
                    registros_totales += 1
                    log.info(f"Estadísticas - Registrado para OF {datos.get('of')}")
                else:
                    log.error(f"Estadísticas - Error insertando para OF {datos.get('of')}")
            
            ciclos_totales += 1
            
            # Estadísticas cada 60 segundos
            if time.time() - ultimo_log_stats >= 60:
                log.info(f"Estadísticas - Estadísticas: {ciclos_totales} ciclos, {registros_totales} registros totales")
                ultimo_log_stats = time.time()
            
            # Esperar para próximo ciclo (3600 segundos = 1 hora)
            ciclo_tiempo = time.time() - ciclo_inicio
            espera = max(300, 3600.0 - ciclo_tiempo)  # Mínimo 5 minutos
            if espera > 0:
                log.info(f"Estadísticas - Esperando {espera:.0f} segundos para próximo ciclo...")
                # Usar sleep con verificación periódica de _stop
                start_wait = time.time()
                while time.time() - start_wait < espera and not _stop:
                    await asyncio.sleep(1)  # Verificar cada segundo
    
    finally:
        await close_opc_client()
    
    log.info("Servicio de estadísticas detenido")

if __name__ == "__main__":
    install_signal_handlers()
    try:
        asyncio.run(run_estadisticas_service())
    except KeyboardInterrupt:
        log.info("Interrumpido por usuario")
    except Exception as e:
        log.error(f"Error fatal: {e}")
        log.exception("Detalles:")
