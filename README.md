# Datalogger Industrial M594

Sistema de recolección de datos industriales para la máquina M594 con servicios especializados para trazabilidad, estadísticas, eventos y OEE.

## 📋 Tabla de Contenidos

- [Visión General](#visión-general)
- [Arquitectura](#arquitectura)
- [Servicios](#servicios)
  - [Trazabilidad](#trazabilidad)
  - [Estadísticas](#estadísticas)
  - [Eventos](#eventos)
  - [OEE](#oee)
- [Configuración](#configuración)
- [Instalación y Ejecución](#instalación-y-ejecución)
- [Monitoreo y Logs](#monitoreo-y-logs)
- [Troubleshooting](#troubleshooting)

## 🎯 Visión General

El datalogger M594 es un sistema modular que recolecta datos de una máquina industrial a través del protocolo OPC UA y los almacena en una base de datos MySQL. El sistema está diseñado para ser robusto, con reconexión automática y manejo de errores.

### Características Principales
- **Recolección en tiempo real** de datos de producción
- **Reconexión automática** ante caídas de red o servicios
- **Modularidad** con servicios independientes
- **Logging detallado** para depuración
- **Terminación controlada** con Ctrl+C

## 🏗️ Arquitectura

```
┌─────────────────┐    ┌─────────────────┐
│   Máquina M594 │────│   Base de     │
│   (PLC con OPC UA) │    │   Datos MySQL  │
│                 │    │                │
└─────────────────┘    └─────────────────┘
         │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────────────────────┐
                    │      Datalogger M594       │
                    │  (Servicios Python)         │
                    │                             │
                    │  • trazabilidad_service.py   │
                    │  • estadisticas_service.py  │
                    │  • eventos_service.py       │
                    │  • oee_service.py          │
                    └─────────────────────────────────┘
```

## 🔧 Servicios

### 📊 Trazabilidad

**Archivo**: `trazabilidad_service.py`

**Propósito**: Registrar cada ciclo de producción con códigos de productos y polos para trazabilidad completa.

**Frecuencia**: Cada 1 segundo (tiempo real)

**Datos que recolecta**:
- Ciclo actual de producción
- Orden de Fabricación (OF) activa
- Códigos de productos terminados
- Códigos de polos (1-4)
- Código de inspecciones
- Turno actual
- Fecha y hora del registro

**Flujo de operación**:
1. Conecta al servidor OPC UA
2. Lee tags de trazabilidad cada segundo
3. Detecta cambios en el ciclo actual
4. Registra cada nuevo ciclo en la base de datos
5. Maneja ciclos perdidos si se detectan saltos
6. Reinicia tracking cuando cambia la OF

**Configuración**:
- Archivo `tags.json`: Mapeo de tags a NodeIds OPC UA
- Variables de entorno: `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DB`, etc.

---

### 📈 Estadísticas

**Archivo**: `estadisticas_service.py`

**Propósito**: Consolidar estadísticas de producción cada hora para análisis de rendimiento.

**Frecuencia**: Cada 3600 segundos (1 hora)

**Datos que recolecta**:
- Total de piezas buenas
- Total de piezas malas
- Producción faltante
- Desglose de defectos por estación (E1, E3, E6, E10, E11, E12, E14)
- Orden de Fabricación (OF)
- Turno actual
- Fecha y hora del registro

**Flujo de operación**:
1. Conecta al servidor OPC UA
2. Espera 1 hora entre ciclos
3. Lee tags de estadísticas
4. Inserta registro consolidado en la base de datos
5. Repite el ciclo

**Configuración**:
- Archivo `tags.json`: Mapeo de tags a NodeIds OPC UA
- Variables de entorno MySQL

---

### 🚨 Eventos

**Archivo**: `eventos_service.py`

**Propósito**: Registrar eventos de parada y alarmas del sistema con su duración y frecuencia.

**Frecuencia**: Cada 60 segundos (1 minuto)

**Datos que recolecta**:
- ID del evento
- Categoría del evento
- Cantidad de ocurrencias
- Tiempo acumulado en segundos
- Orden de Fabricación (OF)
- Turno actual
- Fecha y hora del evento
- Fecha y hora de registro

**Flujo de operación**:
1. Conecta al servidor OPC UA
2. Lee configuración desde `config_tablas_especializadas.json`
3. Lee tags de eventos cada minuto
4. Procesa y formatea datos de eventos
5. Inserta múltiples eventos en la base de datos
6. Repite el ciclo

**Configuración**:
- Archivo `tags.json`: Mapeo de tags a NodeIds OPC UA
- Archivo `config_tablas_especializadas.json`: Configuración específica de eventos
- Variables de entorno MySQL

---

### 📊 OEE (Overall Equipment Effectiveness)

**Archivo**: `oee_service.py`

**Propósito**: Calcular y registrar indicadores de eficiencia del equipo.

**Frecuencia**: Cada 3600 segundos (1 hora)

**Datos que recolecta**:
- Cantidad de paradas
- Downtime en minutos (interno y externo)
- Disponibilidad (%)
- Performance (%)
- Calidad (%)
- OEE global (%)
- Porcentaje de tiempo de parada
- Porcentaje de scrap
- Orden de Fabricación (OF)
- Turno actual
- Fecha y hora del registro

**Flujo de operación**:
1. Conecta al servidor OPC UA
2. Espera 1 hora entre ciclos
3. Lee tags de OEE
4. Procesa y calcula métricas
5. Inserta registro en la base de datos
6. Repite el ciclo

**Configuración**:
- Archivo `tags.json`: Mapeo de tags a NodeIds OPC UA
- Variables de entorno MySQL

## ⚙️ Configuración

### Archivos de Configuración

#### `tags.json`
```json
{
  "OPC_DATOS.TRAZABILIDAD.CICLO_ACTUAL": "ns=2;s=Demo.Dynamic.Scalar.CicloActual",
  "OPC_DATOS.ESTADISTICAS.BUENAS_TOTALES": "ns=2;s=Demo.Dynamic.Scalar.BuenasTotales",
  ...
}
```
Mapeo entre nombres lógicos de tags y NodeIds del servidor OPC UA.

#### `config_tablas_especializadas.json`
```json
{
  "tablas": {
    "eventos": {
      "tags": [
        {
          "nombre": "E1_QR",
          "tag": "OPC_DATOS.EVENTOS.E1_QR",
          "id_evento": "E1_QR",
          "categoria": "Calidad"
        }
      ]
    }
  }
}
```
Configuración específica para eventos, incluyendo categorías y mapeos.

### Variables de Entorno

Crear archivo `.env` en el directorio raíz:
```bash
# Servidor OPC UA
OPC_ENDPOINT=opc.tcp://192.168.20.30:4840

# Base de datos MySQL
MYSQL_HOST=192.168.0.183
MYSQL_PORT=3306
MYSQL_DB=m594
MYSQL_USER=m594
MYSQL_PASS=594

# Nivel de logging
LOG_LEVEL=INFO
```

### Estructura de Base de Datos

El sistema crea automáticamente las tablas necesarias:
- `trazabilidad`: Registros de producción por ciclo
- `estadisticas`: Consolidados horarios de producción
- `eventos`: Registro de eventos y alarmas
- `oee`: Indicadores de eficiencia

##  Compatibilidad MySQL

### Versiones Soportadas
- **MySQL 5.7+**: Totalmente compatible
- **MySQL 8.0**: Compatible con configuración específica
- **MariaDB 10.3+**: Compatible

### Problemas Conocidos con MySQL 8.0+

#### Error de Autenticación
**Síntoma**: `Authentication plugin 'caching_sha2_password' is not supported`

**Solución 1**: Cambiar método de autenticación en MySQL
```sql
ALTER USER 'm594'@'%' IDENTIFIED WITH mysql_native_password BY '594';
FLUSH PRIVILEGES;
```

**Solución 2**: Usar versión específica del conector
```bash
# Reemplazar en requirements.txt
mysql-connector-python==8.0.33  # Versión compatible con MySQL 8.0+
```

#### Error de SQL Mode
**Síntoma**: `Syntax error due to sql_mode=ONLY_FULL_GROUP_BY`

**Solución**: Configurar SQL mode en MySQL
```sql
SET GLOBAL sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO';
```

### Recomendación
Para máxima compatibilidad, se recomienda **MySQL 5.7** o **MariaDB 10.3+**.

##  Herramientas de Configuración

### generate_tags.py

Este script es esencial para descubrir y mapear automáticamente todos los tags disponibles en el PLC OPC UA.

**Propósito**: Conectar al PLC, explorar su estructura de nodos y generar el archivo `tags.json` que utilizan todos los servicios.

**Flujo de operación**:
1. Conecta al servidor OPC UA del PLC
2. Navega por la estructura jerárquica desde `ROOT_PATH`
3. Descubre recursivamente todos los nodos disponibles
4. Crea mapeo entre nombres legibles y NodeIds
5. Guarda el resultado en `tags.json`

**Configuración**:
```python
# Variables principales en generate_tags.py
ENDPOINT = os.getenv("OPC_ENDPOINT", "opc.tcp://192.168.20.30:4840")
ROOT_PATH = ["ServerInterfaces", "M594_Datalogger", "OPC_DATOS"]
OUTFILE = Path("tags.json")
```

**Uso**:
```bash
# Ejecutar para generar tags.json
python generate_tags.py

# Salida esperada
OK: 156 nodos guardados en /path/to/tags.json
```

**Estructura generada**:
```json
{
  "OPC_DATOS.TRAZABILIDAD.CICLO_ACTUAL": "ns=2;s=Demo.Dynamic.Scalar.CicloActual",
  "OPC_DATOS.ESTADISTICAS.BUENAS_TOTALES": "ns=2;s=Demo.Dynamic.Scalar.BuenasTotales",
  "OPC_DATOS.EVENTOS.E1_QR": "ns=2;s=Demo.Dynamic.Scalar.E1_QR"
}
```

**Cuándo ejecutar**:
- Configuración inicial del sistema
- Después de cambios en la estructura del PLC
- Cuando se agregan nuevos tags al sistema

**Importancia**: Este script es fundamental porque los servicios no pueden operar sin un `tags.json` válido. Es el puente entre los nombres lógicos que usamos en el código y los NodeIds reales del PLC.

## 🚀 Instalación y Ejecución

### Prerrequisitos
- Python 3.8+
- **MySQL Server 5.7 - 8.0** (ver sección de compatibilidad)
- Acceso al PLC con servidor OPC UA integrado (M594)

### Instalación
```bash
# Clonar el repositorio
git clone <repositorio>
cd industrial_datalogger_m594

# Instalar dependencias
pip install -r requirements.txt

# Configurar variables de entorno
cp .env.example .env
# Editar .env con los valores apropiados
```

### Ejecución

#### Ejecutar todos los servicios
```bash
# En terminales separados o usando screen/tmux
python trazabilidad_service.py &
python estadisticas_service.py &
python eventos_service.py &
python oee_service.py &
```

#### Ejecutar servicio específico
```bash
# Trazabilidad
python trazabilidad_service.py

# Estadísticas
python estadisticas_service.py

# Eventos
python eventos_service.py

# OEE
python oee_service.py
```

#### Ejecutar con systemd (recomendado para producción)
Crear archivos de servicio en `/etc/systemd/system/`:

```ini
# /etc/systemd/system/datalogger-trazabilidad.service
[Unit]
Description=Datalogger M594 - Trazabilidad
After=network.target

[Service]
Type=simple
User=datalogger
WorkingDirectory=/opt/datalogger_m594
ExecStart=/usr/bin/python3 /opt/datalogger_m594/trazabilidad_service.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
# Habilitar y iniciar servicios
sudo systemctl enable datalogger-trazabilidad
sudo systemctl enable datalogger-estadisticas
sudo systemctl enable datalogger-eventos
sudo systemctl enable datalogger-oee

sudo systemctl start datalogger-trazabilidad
sudo systemctl start datalogger-estadisticas
sudo systemctl start datalogger-eventos
sudo systemctl start datalogger-oee
```

## 📊 Monitoreo y Logs

### Niveles de Logging
- `DEBUG`: Información detallada de depuración
- `INFO`: Información general de operación
- `WARNING`: Advertencias no críticas
- `ERROR`: Errores que requieren atención
- `CRITICAL`: Errores críticos del sistema

### Logs del Sistema
```bash
# Ver logs en tiempo real
tail -f /var/log/datalogger/trazabilidad.log
tail -f /var/log/datalogger/estadisticas.log
tail -f /var/log/datalogger/eventos.log
tail -f /var/log/datalogger/oee.log

# Ver logs de systemd
sudo journalctl -u datalogger-trazabilidad -f
sudo journalctl -u datalogger-estadisticas -f
```

### Métricas de Monitoreo
Cada servicio muestra estadísticas cada 60 segundos:
- Ciclos procesados
- Registros insertados
- Errores de conexión
- Tiempos de respuesta

## 🔧 Troubleshooting

### Problemas Comunes

#### 1. Conexión OPC UA fallida
**Síntomas**: `Error conectando OPC UA`
**Soluciones**:
- Verificar conectividad: `ping 192.168.20.30`
- Verificar servidor OPC UA: `telnet 192.168.20.30 4840`
- Revisar firewall y reglas de red
- Verificar archivo `tags.json`

#### 2. Conexión MySQL fallida
**Síntomas**: `Error conectando MySQL`
**Soluciones**:
- Verificar servicio MySQL: `systemctl status mysql`
- Probar conexión: `mysql -h 192.168.0.183 -u m594 -p m594`
- Revisar credenciales en `.env`
- Verificar permisos de usuario en base de datos

#### 3. Servicio no termina con Ctrl+C
**Síntomas**: El servicio se queda colgado al intentar detenerlo
**Solución**: Los servicios ya tienen implementada la terminación controlada. Si persiste:
- Verificar que no haya procesos zombies: `ps aux | grep python`
- Forzar terminación: `kill -9 <PID>`

#### 4. Pérdida de datos durante desconexiones
**Síntomas**: Huecos en los datos durante caídas de red
**Solución**: Los servicios tienen reconexión automática implementada. Si hay pérdidas:
- Revisar logs de reconexión
- Verificar tiempo de caída vs frecuencia de muestreo
- Considerar buffering local (no implementado por diseño)

### Depuración

#### Modo DEBUG
```bash
# Ejecutar con logging detallado
LOG_LEVEL=DEBUG python trazabilidad_service.py
```

#### Verificar tags específicos
```python
# Script de prueba para verificar tags
import asyncio
from asyncua import Client

async def test_tags():
    client = Client(url="opc.tcp://192.168.20.30:4840")
    await client.connect()
    
    # Probar tag específico
    try:
        node = client.get_node("ns=2;s=Demo.Dynamic.Scalar.CicloActual")
        value = await node.read_value()
        print(f"Valor: {value}")
    except Exception as e:
        print(f"Error: {e}")
    
    await client.disconnect()

asyncio.run(test_tags())
```

### Mantenimiento

#### Limpieza de logs
```bash
# Rotar logs manualmente
sudo logrotate -f /etc/logrotate.d/datalogger

# Limpiar logs antiguos (más de 30 días)
find /var/log/datalogger -name "*.log" -mtime +30 -delete
```

#### Respaldo de base de datos
```bash
# Respaldo diario
mysqldump -h 192.168.0.183 -u m594 -p m594 > backup_$(date +%Y%m%d).sql
```

## 📞 Soporte

Para reportar problemas o solicitar soporte:
1. Revisar logs del servicio afectado
2. Verificar conectividad de red
3. Documentar el error y pasos para reproducir
4. Proporcionar información del sistema (versión Python, MySQL, etc.)

---

**Versión**: 1.0.0  
**Última actualización**: Abril 2026  
**Licencia**: Propietario
