#!/usr/bin/env python3
"""
Servicio dedicado para eventos.
Script independiente que solo maneja la lectura y registro de datos de eventos.
"""

import os
import json
import time
import asyncio
import logging
import signal
from datetime import datetime as dt
from typing import Dict, Any, Optional, List
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

def ensure_eventos_table():
    """Asegura que la tabla eventos existe"""
    conn = load_mysql_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Verificar si la tabla existe
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = 'eventos'
        """, (MYSQL_DB,))
        
        if cursor.fetchone()[0] == 0:
            # Crear tabla
            cursor.execute("""
                CREATE TABLE eventos (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    id_evento INT,
                    categoria VARCHAR(50),
                    nombre_evento VARCHAR(100),
                    cantidad_eventos INT,
                    tiempo_segundos_acumulado INT,
                    turno INT,
                    fecha_hora DATETIME NOT NULL,
                    INDEX idx_fecha_hora (fecha_hora),
                    INDEX idx_categoria (categoria),
                    INDEX idx_id_evento (id_evento)
                )
            """)
            log.info("Tabla eventos creada")
        else:
            log.debug("Tabla eventos ya existe")
        
        cursor.close()
        return True
        
    except Exception as e:
        log.error(f"Error verificando/creando tabla eventos: {e}")
        return False
    finally:
        conn.close()

def insertar_eventos(lista_eventos: List[Dict[str, Any]]) -> int:
    """Inserta múltiples eventos en la base de datos"""
    if not lista_eventos:
        return 0
    
    conn = load_mysql_connection()
    if not conn:
        return 0
    
    try:
        cursor = conn.cursor()
        
        sql = """
        INSERT INTO eventos (id_evento, categoria, nombre_evento, cantidad_eventos,
                            tiempo_segundos_acumulado, turno, fecha_hora)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        valores_insertados = 0
        for evento in lista_eventos:
            values = (
                evento.get("id_evento"),
                evento.get("categoria"),
                evento.get("nombre_evento"),
                evento.get("cantidad_eventos"),
                evento.get("tiempo_segundos_acumulado"),
                evento.get("turno"),
                evento.get("fecha_hora")
            )
            
            cursor.execute(sql, values)
            valores_insertados += 1
            log.debug(f"Insertado evento: {evento}")
        
        log.info(f"Eventos - Insertados {valores_insertados} eventos")
        return valores_insertados
        
    except Exception as e:
        log.error(f"Error insertando eventos: {e}")
        return 0
    finally:
        cursor.close()
        conn.close()

def procesar_valor_opc_ua(valor: Any) -> Any:
    """
    Procesa valores complejos de OPC UA para convertirlos a tipos simples
    """
    # Si es None, retornar None
    if valor is None:
        return None
    
    # Mejorar detección de ExtensionObject
    valor_str = str(type(valor))
    
    # Si es ExtensionObject (tipo complejo de Siemens)
    if 'ExtensionObject' in valor_str:
        try:
            # Intentar extraer el valor del Body
            if hasattr(valor, 'Body') and valor.Body is not None:
                # Intentar diferentes formatos
                try:
                    # Intentar como string UTF-8
                    decoded = valor.Body.decode('utf-8').strip('\x00')
                    if decoded and any(c.isdigit() for c in decoded):
                        return decoded
                except UnicodeDecodeError:
                    pass
                    
                # Intentar como estructura de fecha
                if len(valor.Body) >= 8:
                    # Formato común de fecha en bytes
                    try:
                        import struct
                        # Intentar little-endian 32-bit
                        year = struct.unpack('<H', valor.Body[0:2])[0]
                        month = valor.Body[2]
                        day = valor.Body[3]
                        hour = valor.Body[4]
                        minute = valor.Body[5]
                        second = valor.Body[6]
                        return f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"
                    except:
                        pass
                
                # Último recurso: string del objeto
                return f"ExtensionObject:{str(valor)[:50]}"
            except:
                return f"ExtensionObject:{str(valor)[:50]}"
        else:
            return str(valor)[:50]  # Limitar longitud
    else:
        return str(valor)
    
    # Manejar tipos específicos de OPC UA
    if hasattr(valor, '__class__'):
        tipo_str = str(type(valor))
        
        # DInt (32-bit signed integer)
        if 'DInt' in tipo_str:
            try:
                return int(valor)
            except (ValueError, TypeError):
                return 0
        
        # UDInt (32-bit unsigned integer)  
        if 'UDInt' in tipo_str:
            try:
                return int(valor)
            except (ValueError, TypeError):
                return 0
        
        # UInt (32-bit unsigned integer)
        if 'UInt' in tipo_str:
            try:
                return int(valor)
            except (ValueError, TypeError):
                return 0
        
        # Float
        if 'Float' in tipo_str:
            try:
                return float(valor)
            except (ValueError, TypeError):
                return 0.0
    
    # Si es un tipo numérico complejo, convertir a simple
    if hasattr(valor, 'real') and hasattr(valor, 'imag'):
        return valor.real
    
    # Si es un array o lista
    if isinstance(valor, (list, tuple)):
        if len(valor) == 0:
            return None
        elif len(valor) == 1:
            return valor[0]
        else:
            return str(valor)
    
    # Si es un diccionario
    if isinstance(valor, dict):
        if len(valor) == 0:
            return None
        elif len(valor) == 1:
            return next(iter(valor.values()))
        else:
            return str(valor)
    
    # Para todos los demás casos, intentar conversión numérica
    if isinstance(valor, (int, float)):
        return valor
    
    # Si es string numérico, convertir
    if isinstance(valor, str):
        valor_limpio = valor.strip()
        if valor_limpio.replace('.', '').replace('-', '').isdigit():
            try:
                if '.' in valor_limpio:
                    return float(valor_limpio)
                else:
                    return int(valor_limpio)
            except ValueError:
                return valor_limpio
    
    # Para todos los demás casos, retornar el valor original
    return valor

async def leer_tags_eventos(tags_mapping: Dict[str, str]) -> Dict[str, Any]:
    """Lee todos los tags que comienzan con OPC_DATOS.REGISTRO_EVENTOS"""
    eventos_tags = [tag for tag in tags_mapping.keys() if tag.startswith("OPC_DATOS.REGISTRO_EVENTOS")]
    
    valores = {}
    client = await get_opc_client()
    
    for tag_key in eventos_tags:
        try:
            nodeid = tags_mapping.get(tag_key)
            if not nodeid:
                log.warning(f"No se encontró NodeId para tag {tag_key}")
                valores[tag_key] = None
                continue
            
            node = client.get_node(nodeid)
            valor = await node.read_value()
            
            # Manejar tipos complejos de OPC UA
            valor_procesado = procesar_valor_opc_ua(valor)
            valores[tag_key] = valor_procesado
            
            log.debug(f"Leído {tag_key}: {valor_procesado}")
            
        except Exception as e:
            log.warning(f"No se pudo leer tag {tag_key}: {e}")
            valores[tag_key] = None
    
    return valores

def reconstruir_fecha_dtl(dtl_componentes: Dict[str, Any]) -> Optional[dt]:
    """Reconstruye fecha desde componentes DTL"""
    try:
        year = dtl_componentes.get("YEAR")
        month = dtl_componentes.get("MONTH")
        day = dtl_componentes.get("DAY")
        hour = dtl_componentes.get("HOUR")
        minute = dtl_componentes.get("MINUTE")
        
        if all(v is not None for v in [year, month, day, hour, minute]):
            return dt(
                year=int(year),
                month=int(month),
                day=int(day),
                hour=int(hour),
                minute=int(minute),
                second=0
            )
    except (ValueError, TypeError) as e:
        log.warning(f"Error reconstruyendo fecha DTL: {e}")
    
    return None

def construir_fecha_desde_dtl(valores: Dict[str, Any], base_event: str) -> Optional[dt]:
    """
    Construye un objeto datetime desde componentes DTL individuales
    """
    try:
        # Buscar componentes DTL para este evento
        # El base_event ya incluye el prefijo completo
        year_tag = f"{base_event}.FechaYHora.YEAR"
        month_tag = f"{base_event}.FechaYHora.MONTH"
        day_tag = f"{base_event}.FechaYHora.DAY"
        hour_tag = f"{base_event}.FechaYHora.HOUR"
        minute_tag = f"{base_event}.FechaYHora.MINUTE"
        
        # Obtener valores
        year = valores.get(year_tag)
        month = valores.get(month_tag)
        day = valores.get(day_tag)
        hour = valores.get(hour_tag)
        minute = valores.get(minute_tag)
        
        # Validar que tengamos los componentes necesarios
        if all(v is not None for v in [year, month, day, hour, minute]):
            # Convertir a enteros
            year = int(year)
            month = int(month)
            day = int(day)
            hour = int(hour)
            minute = int(minute)
            
            # Crear datetime con cualquier fecha del PLC (incluyendo 1970-01-01)
            fecha_dt = dt(year, month, day, hour, minute, 0)
            log.debug(f"Fecha reconstruida para {base_event}: {fecha_dt}")
            return fecha_dt
        else:
            log.warning(f"Faltan componentes DTL para {base_event}: Y={year}, M={month}, D={day}, H={hour}, M={minute}")
            return None
            
    except Exception as e:
        log.error(f"Error construyendo fecha para {base_event}: {e}")
        return None

def procesar_eventos(valores: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Procesa datos de eventos y devuelve lista de eventos para inserción"""
    eventos_registrados = []
    eventos_base = {}
    ahora = dt.now()
    
    for tag, valor in valores.items():
        if valor is not None:
            # Extraer nombre base del evento y tipo de campo
            partes_tag = tag.split(".")
            
            # Detectar componentes DTL de fecha
            if len(partes_tag) >= 6 and partes_tag[-1] in ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE"]:
                # Es un componente DTL de fecha
                evento_base = ".".join(partes_tag[:-2])  # Ej: OPC_DATOS.REGISTRO_EVENTOS.FALLAS.02_E4_FALLA_SENSOR_SEGURIDAD_REMACHADO.FechaYHora
                componente = partes_tag[-1]  # YEAR, MONTH, etc.
                
                # Extraer el nombre real del evento (sin FechaYHora)
                evento_real = ".".join(partes_tag[:-3])  # Quita .FechaYHora.COMPONENTE
                
                if evento_real not in eventos_base:
                    eventos_base[evento_real] = {}
                    eventos_base[evento_real]["_dtl_componentes"] = {}
                
                # Guardar componente DTL
                eventos_base[evento_real]["_dtl_componentes"][componente] = valor
            
            elif len(partes_tag) >= 4 and partes_tag[-1] in ["Categoria", "ID", "CantidadEventos", "Turno", "TiempoSegundosAcumulado"]:
                # Es un campo normal del evento
                evento_base = ".".join(partes_tag[:-1])  # Ej: OPC_DATOS.REGISTRO_EVENTOS.FALLAS.06_E15_POLO_ATASCADO
                campo = partes_tag[-1].lower()
                
                if evento_base not in eventos_base:
                    eventos_base[evento_base] = {}
                
                # Mapear campo a nombre de columna
                if campo == "categoria":
                    eventos_base[evento_base]["categoria"] = valor
                elif campo == "id":
                    eventos_base[evento_base]["id_evento"] = valor
                elif campo == "cantidadeventos":
                    eventos_base[evento_base]["cantidad_eventos"] = valor
                elif campo == "turno":
                    eventos_base[evento_base]["turno"] = valor
                elif campo == "tiemposegundosacumulado":
                    eventos_base[evento_base]["tiempo_segundos_acumulado"] = valor
    
    # Crear un registro por cada evento que tenga datos
    for evento_base, datos_evento in eventos_base.items():
        # Solo procesar eventos que tengan datos reales (no solo componentes DTL)
        if datos_evento and len([k for k in datos_evento.keys() if k != "_dtl_componentes"]) > 0:
            log.debug(f"Procesando evento: {evento_base}")
            
            datos_completos = {
                "fecha_hora": ahora,
                "fecha_hora_registro": ahora,
                "of": valores.get("OPC_DATOS.GENERAL.OF"),
                "turno": valores.get("OPC_DATOS.GENERAL.TURNO_ACTUAL")
            }
            
            # Agregar datos del evento
            for key, valor in datos_evento.items():
                if key != "_dtl_componentes":
                    datos_completos[key] = valor
            
            # Reconstruir fecha desde componentes DTL si existen
            if "_dtl_componentes" in datos_evento:
                log.debug(f"  Componentes DTL encontrados: {datos_evento['_dtl_componentes']}")
                # Usar el evento_base completo para construir los tags
                fecha_reconstruida = construir_fecha_desde_dtl(valores, evento_base)
                
                if fecha_reconstruida:
                    datos_completos["fecha_y_hora"] = fecha_reconstruida
                    log.debug(f"Fecha reconstruida para {evento_base}: {fecha_reconstruida}")
                else:
                    # No se pudo reconstruir fecha, dejar como None
                    datos_completos["fecha_y_hora"] = None
                    log.debug(f"No se pudieron reconstruir componentes DTL para {evento_base}")
            else:
                log.debug(f"  No se encontraron componentes DTL para {evento_base}")
                # Intentar buscar componentes DTL directamente en valores
                dtl_components = {}
                for comp in ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE"]:
                    tag = f"{evento_base}.FechaYHora.{comp}"
                    if tag in valores:
                        dtl_components[comp] = valores[tag]
                    
                if dtl_components:
                    log.debug(f"  Componentes DTL encontrados directamente: {dtl_components}")
                    # Reconstruir fecha con estos componentes
                    valores_temp = valores.copy()
                    for comp, val in dtl_components.items():
                        tag = f"{evento_base}.FechaYHora.{comp}"
                        valores_temp[tag] = val
                    
                    fecha_reconstruida = construir_fecha_desde_dtl(valores_temp, evento_base)
                    if fecha_reconstruida:
                        datos_completos["fecha_y_hora"] = fecha_reconstruida
                        log.debug(f"Fecha reconstruida (directa) para {evento_base}: {fecha_reconstruida}")
            
            eventos_registrados.append(datos_completos)
    
    log.debug(f"Procesados {len(eventos_registrados)} eventos")
    
    # Convertir al formato esperado por la función insertar_eventos
    eventos_formateados = []
    for evento in eventos_registrados:
        evento_formateado = {
            "id_evento": evento.get("id_evento"),
            "categoria": evento.get("categoria"),
            "nombre_evento": evento_base.split(".")[-1] if "evento_base" in locals() else "desconocido",
            "cantidad_eventos": evento.get("cantidad_eventos"),
            "tiempo_segundos_acumulado": evento.get("tiempo_segundos_acumulado"),
            "turno": evento.get("turno"),
            "fecha_hora": evento.get("fecha_y_hora", evento.get("fecha_hora_registro", dt.now()))
        }
        eventos_formateados.append(evento_formateado)
    
    log.info(f"Eventos - Procesados {len(eventos_formateados)} eventos válidos")
    return eventos_formateados

async def run_eventos_service():
    """Servicio principal de eventos"""
    log.info("Iniciando servicio de eventos...")
    log.info(f"OPC UA: {OPC_ENDPOINT}")
    log.info(f"MySQL: {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}")
    
    # Verificar tabla
    if not ensure_eventos_table():
        log.error("No se pudo verificar/crear tabla eventos")
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
    
    log.info("Iniciando ciclo de eventos (cada 60 segundos)...")
    
    try:
        while not _stop:
            ciclo_inicio = time.time()
            
            # Leer tags de eventos
            valores = await leer_tags_eventos(tags_mapping)
            
            # Procesar eventos
            eventos = procesar_eventos(valores)
            
            if eventos:
                insertados = insertar_eventos(eventos)
                registros_totales += insertados
            
            ciclos_totales += 1
            
            # Estadísticas cada 60 segundos
            if time.time() - ultimo_log_stats >= 60:
                log.info(f"Eventos - Estadísticas: {ciclos_totales} ciclos, {registros_totales} registros totales")
                ultimo_log_stats = time.time()
            
            # Esperar para próximo ciclo (60 segundos)
            ciclo_tiempo = time.time() - ciclo_inicio
            espera = max(30, 60.0 - ciclo_tiempo)  # Mínimo 30 segundos
            if espera > 0:
                await asyncio.sleep(espera)
    
    finally:
        await close_opc_client()
    
    log.info("Servicio de eventos detenido")

if __name__ == "__main__":
    install_signal_handlers()
    try:
        asyncio.run(run_eventos_service())
    except KeyboardInterrupt:
        log.info("Interrumpido por usuario")
    except Exception as e:
        log.error(f"Error fatal: {e}")
        log.exception("Detalles:")
