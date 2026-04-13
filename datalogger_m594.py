#!/usr/bin/env python3
"""
datalogger_m594.py

Servicio optimizado con múltiples tablas especializadas.
Cada tabla tiene su propio ciclo de lectura y solo lee los tags necesarios.
"""

from __future__ import annotations

import os
import json
import time
import signal
import logging
import asyncio
from pathlib import Path
from typing import Dict, Any, List, Optional
from decimal import Decimal
from datetime import datetime as dt
import mysql.connector
from mysql.connector import Error
import asyncua
from asyncua import Client

# Configurar logging para silenciar librerías externas
logging.getLogger("asyncua").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.getLogger("mysql.connector").setLevel(logging.WARNING)

# Silenciar advertencia específica de timeout de sesión OPC UA
logging.getLogger("asyncua.client.ua_client.UaClient").setLevel(logging.ERROR)

# Filtro para advertencia específica de timeout
class TimeoutFilter(logging.Filter):
    def filter(self, record):
        return "session timeout" not in record.getMessage().lower()

# Aplicar filtro a todos los loggers
for logger_name in logging.root.manager.loggerDict:
    logger = logging.getLogger(logger_name)
    logger.addFilter(TimeoutFilter())

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# -------------------------
# Configuración
# -------------------------

OPC_ENDPOINT = os.getenv("OPC_ENDPOINT", "opc.tcp://192.168.0.195:4840")
CONFIG_FILE = Path(os.getenv("CONFIG_FILE", "config_tablas_especializadas.json"))

MYSQL_HOST = os.getenv("MYSQL_HOST", "192.168.0.195")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB = os.getenv("MYSQL_DB", "m594")
MYSQL_USER = os.getenv("MYSQL_USER", "m594")
MYSQL_PASS = os.getenv("MYSQL_PASS", "594")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("opc_tablas")

# -------------------------
# Gestión de estado global
# -------------------------

_stop = False

def _handle_stop(sig, frame):
    global _stop
    _stop = True
    log.info("Señal de parada recibida (%s). Cerrando...", sig)

def install_signal_handlers():
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(s, _handle_stop)
        except Exception:
            pass

# -------------------------
# MySQL
# -------------------------

def create_connection():
    """Crea una conexión directa a MySQL"""
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        database=MYSQL_DB,
        autocommit=False,
        connect_timeout=10
    )

def ensure_tabla(table_name: str, schema: Dict[str, Any]) -> None:
    """Crea la tabla si no existe"""
    columns = schema["columns"]
    
    # Crear DDL para columnas
    column_defs = []
    for col in columns:
        column_defs.append(f"`{col['name']}` {col['type']}")
    
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
      {',\n  '.join(column_defs)}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    
    cn = create_connection()
    try:
        cur = cn.cursor()
        cur.execute(ddl)
        cn.commit()
        log.info(f"Tabla {table_name} verificada/creada")
        
        # Crear índices
        indexes = schema.get("indexes", [])
        for index in indexes:
            index_name = index["name"]
            index_cols = index["columns"]
            try:
                # Intentar crear el índice directamente
                index_ddl = f"""
                CREATE INDEX `{index_name}` 
                ON `{table_name}` ({','.join(['`'+c+'`' for c in index_cols])})
                """
                cur.execute(index_ddl)
                cn.commit()
                log.info(f"Índice {index_name} creado/verificado")
            except mysql.connector.Error as e:
                # Si el índice ya existe, ignorar el error
                if "duplicate key name" in str(e).lower() or "already exists" in str(e).lower():
                    log.info(f"Índice {index_name} ya existe")
                else:
                    log.warning(f"No se pudo crear índice {index_name}: {e}")
                cn.rollback()
                
    finally:
        cn.close()

def insertar_datos_tabla(table_name: str, datos: Dict[str, Any], schema: Dict[str, Any]) -> bool:
    """Inserta datos en una tabla específica"""
    if not datos:
        return False
    
    # Construir INSERT dinámico según la tabla
    # Para tabla eventos, incluir fecha_hora_registro, para otras excluir id y fecha_hora_registro
    if table_name == "eventos":
        columns = [col["name"] for col in schema["columns"] 
                    if col["name"] != "id"]  # Incluir todas menos id
    else:
        columns = [col["name"] for col in schema["columns"] 
                    if col["name"] not in ["id", "fecha_hora_registro"]]  # Excluir id y fecha_hora_registro
    
    placeholders = ["%s"] * len(columns)
    
    sql = f"""
    INSERT INTO `{table_name}` ({', '.join([f'`{col}`' for col in columns])})
    VALUES ({', '.join(placeholders)})
    """
    
    values = []
    for col in columns:
        value = datos.get(col)
        if value is None:
            values.append(None)
        elif isinstance(value, dt):
            values.append(value)
        elif isinstance(value, Decimal):
            values.append(float(value))
        elif isinstance(value, str):
            # Limitar longitud de strings según el tipo de columna
            max_length = 255  # valor por defecto
            for col_schema in schema["columns"]:
                if col_schema["name"] == col and "VARCHAR(" in col_schema["type"]:
                    try:
                        max_length = int(col_schema["type"].split("(")[1].split(")")[0])
                    except:
                        pass
                    break
            values.append(value[:max_length] if len(value) > max_length else value)
        else:
            # Para tipos numéricos complejos o ExtensionObject ya procesados
            try:
                if isinstance(value, (int, float)):
                    values.append(value)
                else:
                    # Intentar convertir a número si es posible
                    if isinstance(value, str) and value.replace('.', '').replace('-', '').isdigit():
                        if '.' in value:
                            values.append(float(value))
                        else:
                            values.append(int(value))
                    else:
                        values.append(str(value))  # Último recurso: convertir a string
            except (ValueError, TypeError):
                values.append(str(value))  # Último recurso: convertir a string
    
    cn = create_connection()
    try:
        cur = cn.cursor()
        cur.execute(sql, values)
        cn.commit()
        log.info(f"¡ Insertado en {table_name}: {datos}")
        return True
    except Exception as e:
        cn.rollback()
        log.error(f"¡ Error insertando en {table_name}: {e}")
        log.error(f"¡ Datos que causaron error: {datos}")
        log.error(f"¡ SQL: {sql}")
        log.error(f"¡ Values: {values}")
        return False
    finally:
        cn.close()

# -------------------------
# Carga de configuración
# -------------------------

def load_config(path: Path) -> Dict[str, Any]:
    """Carga configuración de tablas desde JSON"""
    if not path.exists():
        raise FileNotFoundError(f"No existe {path}.")
    
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or not data:
        raise ValueError("config_tablas_especializadas.json inválido o vacío.")
    
    return data

def load_tags_mapping() -> Dict[str, str]:
    """Carga el mapeo de tags a NodeIds"""
    tags_path = Path("tags.json")
    if not tags_path.exists():
        raise FileNotFoundError("No se encuentra tags.json")
    
    with open(tags_path, 'r', encoding='utf-8') as f:
        return json.load(f)

# -------------------------
# Estado por tabla
# -------------------------

class TablaStateTracker:
    """Maneja el estado por cada tabla"""
    
    def __init__(self):
        self.estados: Dict[str, Dict[str, Any]] = {}
        self.ultimos_registros: Dict[str, dt] = {}
    
    def get_estado(self, tabla: str, key: str) -> Any:
        """Obtiene el estado actual de un tag en una tabla"""
        return self.estados.get(f"{tabla}.{key}")
    
    def set_estado(self, tabla: str, key: str, valor: Any) -> None:
        """Establece el estado de un tag en una tabla"""
        self.estados[f"{tabla}.{key}"] = valor
    
    def get_ultimo_registro(self, tabla: str) -> Optional[dt]:
        """Obtiene el timestamp del último registro de una tabla"""
        return self.ultimos_registros.get(tabla)
    
    def set_ultimo_registro(self, tabla: str, timestamp: dt) -> None:
        """Establece el timestamp del último registro de una tabla"""
        self.ultimos_registros[tabla] = timestamp

# -------------------------
# Procesamiento por tabla
# -------------------------

async def leer_tags_tabla(tags: List[str], mapeo: Dict[str, str]) -> Dict[str, Any]:
    """Lee solo los tags necesarios para una tabla específica"""
    valores = {}
    async with Client(url=OPC_ENDPOINT) as client:
        for tag_key in tags:
            try:
                nodeid = mapeo.get(tag_key)
                if not nodeid:
                    log.warning(f"⚠️  No se encontró NodeId para tag {tag_key}")
                    valores[tag_key] = None
                    continue
                
                node = client.get_node(nodeid)
                valor = await node.read_value()
                
                # Manejar tipos complejos de OPC UA
                valor_procesado = procesar_valor_opc_ua(valor)
                valores[tag_key] = valor_procesado
                
                log.debug(f"✅ Leído {tag_key}: {valor_procesado}")
                
            except Exception as e:
                log.warning(f"❌ No se pudo leer tag {tag_key}: {e}")
                valores[tag_key] = None
    
    return valores

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

def procesar_valor_opc_ua(valor: Any) -> Any:
    """
    Procesa valores complejos de OPC UA para convertirlos a tipos simples
    """
    # Si es None, retornar None
    if valor is None:
        return None
    
    # Mejorar detección de ExtensionObject
    valor_str = str(type(valor))
    if 'ExtensionObject' in valor_str or hasattr(valor, '__dict__'):
        try:
            # Intentar extraer atributos de fecha/hora
            if hasattr(valor, 'Year') and hasattr(valor, 'Month') and hasattr(valor, 'Day'):
                return f"{valor.Year:04d}-{valor.Month:02d}-{valor.Day:02d} {valor.Hour:02d}:{valor.Minute:02d}:{valor.Second:02d}"
            elif hasattr(valor, 'Name'):
                return str(valor.Name)
            elif hasattr(valor, 'Value'):
                return procesar_valor_opc_ua(valor.Value)
            else:
                # Para ExtensionObject complejos, intentar extraer del Body
                if hasattr(valor, 'Body') and valor.Body:
                    try:
                        # Intentar decodificar como bytes
                        if isinstance(valor.Body, bytes):
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
                        return f"ExtensionObject:{valor_str[:50]}"
                    except:
                        return f"ExtensionObject:{valor_str[:50]}"
                else:
                    return str(valor)
        except Exception as e:
            log.warning(f"⚠️  No se pudo procesar ExtensionObject: {e}")
            return str(valor)[:50]  # Limitar longitud
    
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

def procesar_trazabilidad(valores: Dict[str, Any], state_tracker: TablaStateTracker) -> bool:
    """Procesa trazabilidad con condición especial"""
    ciclo_actual = valores.get("OPC_DATOS.TRAZABILIDAD.CICLO_ACTUAL")
    of_actual = valores.get("OPC_DATOS.GENERAL.OF")
    
    log.info(f"🔄 Trazabilidad - Ciclo: {ciclo_actual}, OF: {of_actual}")
    
    if ciclo_actual is None or of_actual is None:
        log.warning(f"❌ Trazabilidad - Datos incompletos: ciclo={ciclo_actual}, of={of_actual}")
        return False
    
    # Obtener último estado
    ultimo_ciclo = state_tracker.get_estado("trazabilidad", "ciclo_actual")
    ultima_of = state_tracker.get_estado("trazabilidad", "of")
    
    log.info(f"📊 Trazabilidad - Último ciclo: {ultimo_ciclo}, Última OF: {ultima_of}")
    
    debe_registrar = False
    
    if ultimo_ciclo is None or ultima_of is None:
        # Primera vez
        debe_registrar = True
        log.info(f"  Trazabilidad - Primera vez, se registrará")
    else:
        # Verificar si cambió la OF
        if ultima_of != of_actual:
            # Resetear tracking para nueva OF
            log.info(f"  Trazabilidad - OF cambió ({ultima_of} -> {of_actual}), reseteando tracking")
            state_tracker.set_estado("trazabilidad", "ciclo_actual", None)
            state_tracker.set_estado("trazabilidad", "of", None)
            debe_registrar = True
            log.info(f"  Trazabilidad - Nueva OF, se registrará primer ciclo")
        else:
            # Condición: misma OF y ciclo mayor
            if (ultima_of == of_actual and 
                isinstance(ciclo_actual, (int, float)) and 
                isinstance(ultimo_ciclo, (int, float)) and
                ciclo_actual > ultimo_ciclo):
                debe_registrar = True
                log.info(f"  Trazabilidad - Ciclo mayor ({ciclo_actual} > {ultimo_ciclo}), se registrará")
            else:
                log.info(f"  Trazabilidad - No se registra: misma OF={ultima_of == of_actual}, ciclo mayor={ciclo_actual > ultimo_ciclo if isinstance(ciclo_actual, (int, float)) and isinstance(ultimo_ciclo, (int, float)) else 'N/A'}")
    
    if debe_registrar:
        # Preparar datos para inserción
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
        
        # Actualizar estados
        state_tracker.set_estado("trazabilidad", "ciclo_actual", ciclo_actual)
        state_tracker.set_estado("trazabilidad", "of", of_actual)
        
        return datos
    
    return False

def procesar_intervalo_programado(valores: Dict[str, Any], tabla_config: Dict[str, Any], 
                              state_tracker: TablaStateTracker, tabla_nombre: str) -> Optional[Dict[str, Any]]:
    """Procesa tablas con intervalo programado"""
    intervalo_segundos = tabla_config.get("poll_seconds", 3600)
    
    # Verificar si ya pasó el intervalo
    ultimo_registro = state_tracker.get_ultimo_registro(tabla_nombre)
    ahora = dt.now()
    
    if ultimo_registro and (ahora - ultimo_registro).total_seconds() < intervalo_segundos:
        return None  # Aún no ha pasado el intervalo
    
    # Preparar datos según la tabla
    datos = {
        "fecha_hora": ahora,
        "fecha_hora_registro": ahora,  # Agregando timestamp de registro
        "of": valores.get("OPC_DATOS.GENERAL.OF"),
        "turno": valores.get("OPC_DATOS.GENERAL.TURNO_ACTUAL")
    }
    
    # Mapear tags a campos según la tabla
    if tabla_nombre == "eventos":
        # Para eventos, agrupar por evento base y crear múltiples registros
        eventos_registrados = []
        eventos_base = {}
        
        for tag in tabla_config["tags"]:
            valor = valores.get(tag)
            log.info(f"Evento {tag}: {valor} (tipo: {type(valor)})")
            
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
        
        # Actualizar timestamp del último registro
        state_tracker.set_ultimo_registro(tabla_nombre, ahora)
        
        return eventos_registrados
    
    # Para otras tablas, procesar normalmente
    for tag in tabla_config["tags"]:
        valor = valores.get(tag)
        if valor is not None:
            # Convertir nombre de tag a nombre de campo
            campo_nombre = tag.split(".")[-1].lower()  # Ej: CANT_PARADAS -> cant_paradas
            
            # Log específico para OEE
            if tabla_nombre == "oee":
                log.info(f"  Procesando tag: {tag} -> {campo_nombre} = {valor}")
            
            # Casos especiales para mapeo correcto
            if "DownTime_Minutos_Externo" in tag:
                campo_nombre = "downtime_minutos_externo"
                if tabla_nombre == "oee":
                    log.info(f"  ¡Detectado DownTime_Minutos_Externo! -> {campo_nombre}")
            elif "DownTime_Minutos" in tag:
                campo_nombre = "downtime_minutos"
            elif "PorcentajeStop" in tag:
                campo_nombre = "porcentaje_stop"
            elif "PorcentajeScrap" in tag:
                campo_nombre = "porcentaje_scrap"
            elif "MalasPor_" in tag:
                campo_nombre = "malas_por_" + tag.split("MalasPor_")[1].lower()
            elif "BuenasTotales" in tag:
                campo_nombre = "buenas_totales"
            elif "MalasTotales" in tag:
                campo_nombre = "malas_totales"
            elif "ProduccionFaltante" in tag:
                campo_nombre = "produccion_faltante"
            # Casos especiales para eventos
            elif ".Categoria" in tag:
                campo_nombre = "categoria"
            elif ".ID" in tag:
                campo_nombre = "id_evento"
            elif ".CantidadEventos" in tag:
                campo_nombre = "cantidad_eventos"
            elif ".FechaYHora" in tag:
                campo_nombre = "fecha_y_hora"
            elif ".TiempoSegundosAcumulado" in tag:
                campo_nombre = "tiempo_segundos_acumulado"
            
            # Procesar valor según tipo
            valor_procesado = procesar_valor_opc_ua(valor)
            log.debug(f"🔍 Procesado {campo_nombre}: {valor_procesado} (tipo: {type(valor_procesado)})")
            
            datos[campo_nombre] = valor_procesado
    
    # Log específico para estadísticas
    if tabla_nombre == "estadisticas":
        log.info(f"📊 Datos estadísticas a procesar:")
        for key, val in datos.items():
            if key in ["buenas_totales", "malas_totales", "produccion_faltante"]:
                log.info(f"  {key}: {val} (tipo: {type(val)})")
    
    # Log específico para OEE
    if tabla_nombre == "oee":
        log.info(f"🔍 Datos OEE a procesar:")
        for key, val in datos.items():
            if key in ["downtime_minutos", "downtime_minutos_externo", "cant_paradas", "disponibilidad", "performance", "calidad", "oee"]:
                log.info(f"  {key}: {val} (tipo: {type(val)})")
        
        # Log adicional para verificar tags originales
        log.info(f"🔍 Tags originales OEE:")
        tag_externo = "OPC_DATOS.OEE.DownTime_Minutos_Externo"
        valor_externo = valores.get(tag_externo)
        log.info(f"  {tag_externo}: {valor_externo} (tipo: {type(valor_externo)})")
        tag_normal = "OPC_DATOS.OEE.DownTime_Minutos"
        valor_normal = valores.get(tag_normal)
        log.info(f"  {tag_normal}: {valor_normal} (tipo: {type(valor_normal)})")
    
    # Actualizar timestamp del último registro
    state_tracker.set_ultimo_registro(tabla_nombre, ahora)
    
    return datos

# -------------------------
# Servicio principal
# -------------------------

async def run_tablas_service():
    """Servicio principal con múltiples tablas especializadas"""
    log.info("🚀 Iniciando servicio de tablas especializadas")
    #log.info(f"📡 Endpoint OPC: {OPC_ENDPOINT}")
    log.info(f"📄 Config: {CONFIG_FILE.resolve()}")
    log.info(f"🗄️  MySQL: {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}")
    
    # Cargar configuración
    config = load_config(CONFIG_FILE)
    tags_mapping = load_tags_mapping()
    
    # Crear/verificar tablas
    for tabla_nombre, tabla_config in config["tablas"].items():
        ensure_tabla(tabla_nombre, tabla_config["table_schema"])
    
    # Estado para control de tiempo
    state_tracker = TablaStateTracker()
    
    # Estadísticas
    ciclos_totales = 0
    registros_totales = 0
    ultimo_log_stats = time.time()
    
    # Seguimiento individual de tiempos por tabla
    ultimo_proceso = {tabla: 0 for tabla in config["tablas"].keys()}
    
    log.info("Iniciando ciclo de procesamiento de tablas...")
    
    while not _stop:
        ciclo_inicio = time.time()
        tiempo_actual = time.time()
        
        for tabla_nombre, tabla_config in config["tablas"].items():
            try:
                # Verificar si es momento de procesar esta tabla
                poll_seconds = tabla_config.get("poll_seconds", 60)
                
                # Para trazabilidad, siempre procesar (tiene su propia lógica interna)
                if tabla_nombre == "trazabilidad":
                    debe_procesar = True
                else:
                    # Para otras tablas, respetar poll_seconds
                    tiempo_desde_ultimo = tiempo_actual - ultimo_proceso[tabla_nombre]
                    debe_procesar = tiempo_desde_ultimo >= poll_seconds
                
                if debe_procesar:
                    log.debug(f"Procesando tabla {tabla_nombre} (poll: {poll_seconds}s)")
                    
                    # Leer solo los tags necesarios para esta tabla
                    valores = await leer_tags_tabla(tabla_config["tags"], tags_mapping)
                    
                    # Procesar según la condición
                    condicion = tabla_config.get("condicion", "siempre")
                    
                    if tabla_nombre == "trazabilidad":
                        datos = procesar_trazabilidad(valores, state_tracker)
                    elif condicion == "intervalo_programado":
                        datos = procesar_intervalo_programado(valores, tabla_config, state_tracker, tabla_nombre)
                    else:
                        datos = None
                    
                    # Insertar si hay datos
                    if datos:
                        if tabla_nombre == "eventos" and isinstance(datos, list):
                            # Eventos puede insertar múltiples registros
                            for evento_datos in datos:
                                if insertar_datos_tabla(tabla_nombre, evento_datos, tabla_config["table_schema"]):
                                    registros_totales += 1
                        else:
                            if insertar_datos_tabla(tabla_nombre, datos, tabla_config["table_schema"]):
                                registros_totales += 1
                    
                    # Actualizar timestamp de último procesamiento
                    ultimo_proceso[tabla_nombre] = tiempo_actual
                
            except Exception as e:
                log.error(f"Error procesando tabla {tabla_nombre}: {e}")
                log.exception("Detalles del error:")
        
        ciclos_totales += 1
        
        # Estadísticas cada 60 segundos
        if time.time() - ultimo_log_stats >= 60:
            log.info(f"Estadísticas: {ciclos_totales} ciclos, {registros_totales} registros totales")
            ultimo_log_stats = time.time()
        
        # Esperar para próximo ciclo (siempre 1 segundo para trazabilidad)
        ciclo_tiempo = time.time() - ciclo_inicio
        espera = max(1, 1 - ciclo_tiempo)  # Mínimo 1 segundo
        if espera > 0:
            await asyncio.sleep(espera)
    
    log.info("🛑 Servicio de tablas detenido")

if __name__ == "__main__":
    install_signal_handlers()
    try:
        asyncio.run(run_tablas_service())
    except KeyboardInterrupt:
        log.info("👋 Interrumpido por usuario")
    except Exception as e:
        log.error(f"💥 Error fatal: {e}")
        log.exception("Detalles:")
