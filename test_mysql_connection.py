#!/usr/bin/env python3
"""
Test script para verificar conexión MySQL
"""

import mysql.connector

# Configuración directa
MYSQL_HOST = "192.168.0.195"
MYSQL_PORT = 3306
MYSQL_DB = "m594"
MYSQL_USER = "m594"
MYSQL_PASS = "594"

def test_connection():
    print(f"Probando conexión a MySQL:")
    print(f"  Host: {MYSQL_HOST}:{MYSQL_PORT}")
    print(f"  Usuario: {MYSQL_USER}")
    print(f"  Database: {MYSQL_DB}")
    print()
    
    try:
        # Intentar conexión sin pool primero
        print("1. Probando conexión directa...")
        cn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASS,
            database=MYSQL_DB,
            connect_timeout=10,
            autocommit=True
        )
        
        print("✅ Conexión directa exitosa")
        
        # Probar query simple
        cursor = cn.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
        print(f"✅ Versión MySQL: {version}")
        cursor.execute("SELECT CicloReal FROM paradaspreensamble LIMIT 1")
        ciclo_actual = cursor.fetchall()
        print(f"✅ Ciclo actual: {ciclo_actual}")
        cursor.execute("SELECT DATABASE()")
        db = cursor.fetchone()[0]
        print(f"✅ Database actual: {db}")
        
        cursor.close()
        cn.close()
        
    except mysql.connector.Error as e:
        print(f"❌ Error MySQL: {e.errno} ({e.sqlstate}): {e.msg}")
        return False
    except Exception as e:
        print(f"❌ Error general: {e}")
        return False
    
    try:
        print("\n2. Probando conexión con pool...")
        from mysql.connector import pooling
        
        pool = pooling.MySQLConnectionPool(
            pool_name="test_pool",
            pool_size=1,
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASS,
            database=MYSQL_DB,
            autocommit=False,
        )
        
        cn = pool.get_connection()
        print("✅ Pool connection exitosa")
        
        cursor = cn.cursor()
        cursor.execute("SELECT DATABASE()")
        db = cursor.fetchone()[0]
        print(f"✅ Database actual: {db}")
        cursor.execute("SELECT CicloReal FROM paradaspreensamble LIMIT 1")
        result = cursor.fetchall()
        print(f"✅ Query test: {result}")
        
        cursor.close()
        cn.close()
        
    except mysql.connector.Error as e:
        print(f"❌ Error Pool: {e.errno} ({e.sqlstate}): {e.msg}")
        return False
    except Exception as e:
        print(f"❌ Error Pool general: {e}")
        return False
    
    print("\n✅ Todas las pruebas exitosas")
    return True

if __name__ == "__main__":
    test_connection()
