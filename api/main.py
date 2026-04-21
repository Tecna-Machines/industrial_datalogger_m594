#!/usr/bin/env python3
"""
API principal para el datalogger industrial M594
"""
from __future__ import annotations

import os
import datetime as dt
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from fastapi import FastAPI, HTTPException, Query
from mysql.connector import pooling

MYSQL_HOST = os.getenv("MYSQL_HOST", "192.168.0.183")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB = os.getenv("MYSQL_DB", "m594")
MYSQL_USER = os.getenv("MYSQL_USER", "m594")
MYSQL_PASSWORD = os.getenv("MYSQL_PASS", "594")

app = FastAPI(title="Datalogger M594 API", version="1.0.0")

pool: pooling.MySQLConnectionPool | None = None

def _make_pool():
    global pool
    if pool is None:
        pool = pooling.MySQLConnectionPool(
            pool_name="datalogger_pool",
            pool_size=10,
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            database=MYSQL_DB,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
        )
    return pool

def _row_to_dict(row: tuple, columns: List[str]) -> Dict[str, Any]:
    out = {}
    for c, v in zip(columns, row):
        if isinstance(v, (dt.date, dt.datetime)):
            out[c] = v.isoformat()
        else:
            out[c] = v
    return out

@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = _make_pool()
    yield
    pool = None

app = FastAPI(title="Datalogger M594 API", version="1.0.0", lifespan=lifespan)

@app.get("/health")
def health():
    assert pool is not None
    cn = pool.get_connection()
    try:
        with cn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return {"status": "ok", "database": "connected"}
    finally:
        cn.close()

@app.get("/estadisticas/latest")
def estadisticas_latest():
    assert pool is not None
    cn = pool.get_connection()
    try:
        with cn.cursor() as cur:
            cur.execute("SELECT * FROM estadisticas ORDER BY fecha_hora DESC LIMIT 1")
            row = cur.fetchone()
            if row is None:
                return {"status": "no_data"}
            columns = [d[0] for d in cur.description]
            return _row_to_dict(row, columns)
    finally:
        cn.close()

@app.get("/estadisticas/of/{of}")
def estadisticas_of(of: str):
    assert pool is not None
    cn = pool.get_connection()
    try:
        with cn.cursor() as cur:
            cur.execute("SELECT * FROM estadisticas WHERE `of` = %s ORDER BY fecha_hora DESC", (of,))
            rows = cur.fetchall()
            if len(rows) == 0:
                return {"status": "no_data"}
            columns = [d[0] for d in cur.description]
            return [_row_to_dict(r, columns) for r in rows]
    finally:
        cn.close()

@app.get("/estadisticas/of/{of}/latest")
def estadisticas_of_latest(of: str):
    assert pool is not None
    cn = pool.get_connection()
    try:
        with cn.cursor() as cur:
            cur.execute("SELECT * FROM estadisticas WHERE `of` = %s ORDER BY fecha_hora DESC LIMIT 1", (of,))
            row = cur.fetchone()
            if row is None:
                return {"status": "no_data"}
            columns = [d[0] for d in cur.description]
            return _row_to_dict(row, columns)
    finally:
        cn.close()    

@app.get("/oee/latest")
def oee_latest():
    assert pool is not None
    cn = pool.get_connection()
    try:
        with cn.cursor() as cur:
            cur.execute("SELECT * FROM oee ORDER BY fecha_hora DESC LIMIT 1")
            row = cur.fetchone()
            if row is None:
                return {"status": "no_data"}
            columns = [d[0] for d in cur.description]
            return _row_to_dict(row, columns)
    finally:
        cn.close()    

@app.get("/oee/of/{of}")
def oee_of(of: str):
    assert pool is not None
    cn = pool.get_connection()
    try:
        with cn.cursor() as cur:
            cur.execute("SELECT * FROM oee WHERE `of` = %s ORDER BY fecha_hora DESC", (of,))
            rows = cur.fetchall()
            if len(rows) == 0:
                return {"status": "no_data"}
            columns = [d[0] for d in cur.description]
            return [_row_to_dict(r, columns) for r in rows]
    finally:
        cn.close()  

@app.get("/oee/of/{of}/latest")  
def oee_of_latest(of: str):
    assert pool is not None
    cn = pool.get_connection()
    try:
        with cn.cursor() as cur:
            cur.execute("SELECT * FROM oee WHERE `of` = %s ORDER BY fecha_hora DESC LIMIT 1", (of,))
            row = cur.fetchone()
            if row is None:
                return {"status": "no_data"}
            columns = [d[0] for d in cur.description]
            return _row_to_dict(row, columns)
    finally:
        cn.close()  

@app.get("/eventos/latest")
def eventos_latest():
    assert pool is not None
    cn = pool.get_connection()
    try:
        with cn.cursor() as cur:
            cur.execute("SELECT * FROM eventos ORDER BY fecha_hora_registro DESC LIMIT 37")
            rows = cur.fetchall()
            if len(rows) == 0:
                return {"status": "no_data"}
            columns = [d[0] for d in cur.description]
            return [_row_to_dict(r, columns) for r in rows]
    finally:
        cn.close()

@app.get("/eventos/of/{of}/latest")
def eventos_of_latest(of: str):
    assert pool is not None
    cn = pool.get_connection()
    try:
        with cn.cursor() as cur:
            cur.execute("SELECT * FROM eventos WHERE `of` = %s ORDER BY fecha_hora_registro DESC LIMIT 37", (of,))
            rows = cur.fetchall()
            if len(rows) == 0:
                return {"status": "no_data"}
            columns = [d[0] for d in cur.description]
            return [_row_to_dict(r, columns) for r in rows]
    finally:
        cn.close()

@app.get("/trazabilidad/latest")
def trazabilidad_latest():
    assert pool is not None
    cn = pool.get_connection()
    try:
        with cn.cursor() as cur:
            cur.execute("SELECT * FROM trazabilidad ORDER BY ciclo_actual DESC LIMIT 100")
            rows = cur.fetchall()
            if len(rows) == 0:
                return {"status": "no_data"}
            columns = [d[0] for d in cur.description]
            return [_row_to_dict(r, columns) for r in rows]
    finally:
        cn.close()

@app.get("/trazabilidad/of/{of}")
def trazabilidad_of_latest(of: str):
    assert pool is not None
    cn = pool.get_connection()
    try:
        with cn.cursor() as cur:
            cur.execute("SELECT * FROM trazabilidad WHERE `of` = %s ORDER BY ciclo_actual DESC", (of,))
            rows = cur.fetchall()
            if len(rows) == 0:
                return {"status": "no_data"}
            columns = [d[0] for d in cur.description]
            return [_row_to_dict(r, columns) for r in rows]
    finally:
        cn.close()

