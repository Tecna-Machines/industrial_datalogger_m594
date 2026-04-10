#!/usr/bin/env python3
"""
Script para diagnosticar problemas de conectividad básica
"""

import socket
import subprocess
import platform
import os

def test_basic_connectivity():
    """Pruebas básicas de conectividad"""
    
    # Configuración
    host = "192.168.0.89"
    port = 4840
    
    print(f"=== DIAGNÓSTICO DE CONEXIÓN ===")
    print(f"Host: {host}")
    print(f"Puerto: {port}")
    print(f"Platform: {platform.system()}")
    print("=" * 50)
    
    # 1. Test de ping
    print("1. Test PING:")
    try:
        if platform.system() == "Windows":
            cmd = ["ping", "-n", "4", host]
        else:
            cmd = ["ping", "-c", "4", host]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print("  OK: Ping exitoso")
            print(f"  Salida: {result.stdout.splitlines()[-1]}")
        else:
            print("  ERROR: Ping falló")
            print(f"  Error: {result.stderr}")
    except Exception as e:
        print(f"  ERROR: Ping - {e}")
    
    print()
    
    # 2. Test de conexión TCP
    print("2. Test CONEXIÓN TCP:")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print(f"  OK: Conexión TCP a {host}:{port} exitosa")
        else:
            print(f"  ERROR: No se puede conectar a {host}:{port} (código: {result})")
            print("  Códigos comunes:")
            print("    10061: Conexión rechazada (servidor no escucha)")
            print("    10060: Timeout de conexión")
            print("    10051: Red inalcanzable")
    except Exception as e:
        print(f"  ERROR: Conexión TCP - {e}")
    
    print()
    
    # 3. Test de telnet si está disponible
    print("3. Test TELNET:")
    try:
        if platform.system() == "Windows":
            cmd = ["telnet", host, str(port)]
        else:
            cmd = ["nc", "-zv", host, str(port)]
        
        print(f"  Ejecutando: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        print(f"  Resultado: {result.stdout}")
        if result.stderr:
            print(f"  Error: {result.stderr}")
    except FileNotFoundError:
        print("  INFO: telnet/nc no disponible")
    except Exception as e:
        print(f"  ERROR: Telnet - {e}")
    
    print()
    
    # 4. Verificar interfaces de red
    print("4. Interfaces de red:")
    try:
        if platform.system() == "Windows":
            cmd = ["ipconfig", "/all"]
        else:
            cmd = ["ip", "addr", "show"]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        
        # Buscar la red que contiene la IP del PLC
        lines = result.stdout.splitlines()
        for line in lines:
            if "192.168.0." in line or host in line:
                print(f"  {line.strip()}")
    except Exception as e:
        print(f"  ERROR: Obteniendo interfaces - {e}")
    
    print()
    
    # 5. Verificar si el puerto está siendo usado localmente
    print("5. Puertos en uso local:")
    try:
        if platform.system() == "Windows":
            cmd = ["netstat", "-an"]
        else:
            cmd = ["netstat", "-tlnp"]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        
        lines = result.stdout.splitlines()
        for line in lines:
            if f":{port}" in line or "4840" in line:
                print(f"  {line.strip()}")
    except Exception as e:
        print(f"  ERROR: Verificando puertos - {e}")
    
    print()
    
    # 6. Recomendaciones
    print("6. RECOMENDACIONES:")
    print("  - Verificar que el PLC esté encendido y en la red")
    print("  - Revisar firewall de Windows (desbloquear puerto 4840)")
    print("  - Verificar que el servidor OPC esté corriendo en el PLC")
    print("  - Probar con otro PLC o cambiar la IP si es necesario")
    print("  - Revisar configuración de red (misma subred)")

if __name__ == "__main__":
    test_basic_connectivity()
