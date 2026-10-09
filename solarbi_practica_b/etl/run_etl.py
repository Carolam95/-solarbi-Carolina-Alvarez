"""
=============================================================================
PROYECTO SOLARBI PASCUAL - INTELIGENCIA DE NEGOCIOS (2026-II)
PIPELINE ETL PRINCIPAL (PUNTO DE ENTRADA ÚNICO): etl/run_etl.py
=============================================================================
Flujo:
  1. Lectura de telemetría cruda (Capa Bronze).
  2. Aplicación de 3 Reglas de Calidad (Duplicados, Faltantes, Rango Físico).
  3. Almacenamiento de telemetría curada en data/silver/.
  4. Agregación diaria (Energía en kWh = sum(P_ac * 5/60) y % datos válidos).
  5. Carga Idempotente (UPSERT) en PostgreSQL (silver.lectura_5min y dwh.fact_energia_dia).
  6. Auditoría y verificación de conteos.
=============================================================================
"""

import csv
import os
import sys
from datetime import datetime
from collections import defaultdict

# Configuración de base de datos PostgreSQL desde variables de entorno
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "solarbi")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

# Rutas de archivos
BRONZE_CSV = os.path.join("data", "bronze", "telemetria.csv")
SILVER_CSV = os.path.join("data", "silver", "telemetria_limpia.csv")
SQL_OUTPUT = os.path.join("sql", "carga_idempotente.sql")


def ejecutar_etl():
    print("=" * 70)
    print(">>> INICIANDO PIPELINE ETL SOLARBI PASCUAL <<<")
    print("=" * 70)

    # -------------------------------------------------------------------------
    # PASO 1: VERIFICAR / CARGAR DATOS BRONZE
    # -------------------------------------------------------------------------
    if not os.path.exists(BRONZE_CSV):
        print(f"[WARN] No se encontró {BRONZE_CSV}. Ejecutando simulador...")
        import subprocess
        subprocess.run([sys.executable, os.path.join("etl", "simulador.py")], check=True)

    with open(BRONZE_CSV, "r", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))

    total_leidas = len(reader)
    print(f"\n[PASO 1 - BRONZE] Filas leídas desde {BRONZE_CSV}: {total_leidas}")

    # -------------------------------------------------------------------------
    # PASO 2: APLICAR REGLAS DE CALIDAD DE DATOS (SILVER)
    # -------------------------------------------------------------------------
    # Regla 1: Unicidad (Duplicados por clave ts + dispositivo_id)
    # Regla 2: Completitud (Valores nulos, vacíos o ausentes)
    # Regla 3: Consistencia y Rango Físico (P_ac: [0, 6] kW, Irr: [0, 1400] W/m², Temp: [-10, 90] °C)
    
    seen_keys = set()
    filas_validas = []
    
    rechazos_duplicados = 0
    rechazos_faltantes = 0
    rechazos_rango = 0

    for fila in reader:
        ts_str = fila.get("ts", "").strip()
        disp_id_str = fila.get("dispositivo_id", "").strip()
        p_ac_str = fila.get("p_ac_kw", "").strip()
        irr_str = fila.get("irradiancia_wm2", "").strip()
        temp_str = fila.get("temp_modulo_c", "").strip()

        # REGLA 1: Duplicados
        clave = (ts_str, disp_id_str)
        if clave in seen_keys:
            rechazos_duplicados += 1
            continue
        seen_keys.add(clave)

        # REGLA 2: Datos faltantes
        if not ts_str or not disp_id_str or p_ac_str == "" or irr_str == "" or temp_str == "":
            rechazos_faltantes += 1
            continue

        # REGLA 3: Rango físico válido
        try:
            p_ac = float(p_ac_str)
            irr = float(irr_str)
            temp = float(temp_str)
            disp_id = int(disp_id_str)
        except ValueError:
            rechazos_rango += 1
            continue

        if not (0.0 <= p_ac <= 6.0) or not (0.0 <= irr <= 1400.0) or not (-10.0 <= temp <= 90.0):
            rechazos_rango += 1
            continue

        # Registro válido
        filas_validas.append({
            "ts": ts_str,
            "dispositivo_id": disp_id,
            "p_ac_kw": p_ac,
            "irradiancia_wm2": irr,
            "temp_modulo_c": temp
        })

    total_validas = len(filas_validas)
    total_rechazadas = rechazos_duplicados + rechazos_faltantes + rechazos_rango
    pct_validos = (total_validas / total_leidas * 100) if total_leidas > 0 else 0.0

    print("\n" + "-" * 70)
    print("MÉTRICAS DE CALIDAD DE DATOS (PASO 2 - SILVER):")
    print("-" * 70)
    print(f"  • Filas leídas (Bronze):            {total_leidas:>6}")
    print(f"  • Rechazadas por duplicados:        {rechazos_duplicados:>6}")
    print(f"  • Rechazadas por datos faltantes:    {rechazos_faltantes:>6}")
    print(f"  • Rechazadas por rango físico:      {rechazos_rango:>6}")
    print(f"  • Total filas rechazadas:           {total_rechazadas:>6}")
    print(f"  • Filas válidas (Silver):           {total_validas:>6}")
    print(f"  • Porcentaje de datos válidos:      {pct_validos:>6.2f}%")
    print("-" * 70)

    # Guardar archivo limpio en data/silver/
    os.makedirs(os.path.dirname(SILVER_CSV), exist_ok=True)
    with open(SILVER_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["ts", "dispositivo_id", "p_ac_kw", "irradiancia_wm2", "temp_modulo_c"])
        for r in filas_validas:
            writer.writerow([r["ts"], r["dispositivo_id"], r["p_ac_kw"], r["irradiancia_wm2"], r["temp_modulo_c"]])
    print(f"[OK] Archivo curado guardado en: {SILVER_CSV}")

    # -------------------------------------------------------------------------
    # PASO 3: AGREGACIÓN DIARIA (GOLD / DWH)
    # Cada intervalo es de 5 minutos -> energía_kWh = P_ac_kW * (5 / 60) horas
    # -------------------------------------------------------------------------
    resumen_diario = defaultdict(lambda: {"p_total": 0.0, "validas": 0, "energia_kwh": 0.0})

    for r in filas_validas:
        dt = datetime.fromisoformat(r["ts"])
        fecha_str = dt.strftime("%Y-%m-%d")
        disp_id = r["dispositivo_id"]
        clave_dia = (fecha_str, disp_id)
        
        # Integración por intervalo de 5 minutos
        delta_horas = 5.0 / 60.0
        energia_intervalo = r["p_ac_kw"] * delta_horas
        
        resumen_diario[clave_dia]["energia_kwh"] += energia_intervalo
        resumen_diario[clave_dia]["validas"] += 1

    dwh_filas = []
    print("\n" + "-" * 70)
    print("RESUMEN DIARIO DE ENERGÍA Y CALIDAD (PASO 3 - DWH):")
    print("-" * 70)
    print(f"{'Fecha':<12} | {'Disp':<5} | {'Energía (kWh)':<14} | {'Válidas':<8} | {'Esperadas':<10} | {'% Calidad'}")
    print("-" * 70)

    for (fecha_str, disp_id), datos in sorted(resumen_diario.items()):
        energia_redondeada = round(datos["energia_kwh"], 3)
        filas_val = datos["validas"]
        filas_esp = 288 # 24 horas * 12 lecturas/hora
        pct_dia = round((filas_val / filas_esp) * 100, 2)
        
        dwh_filas.append({
            "fecha": fecha_str,
            "dispositivo_id": disp_id,
            "energia_kwh": energia_redondeada,
            "pct_datos_validos": pct_dia,
            "filas_validas": filas_val,
            "filas_esperadas": filas_esp
        })
        print(f"{fecha_str:<12} | {disp_id:<5} | {energia_redondeada:<14.3f} | {filas_val:<8} | {filas_esp:<10} | {pct_dia:>6.2f}%")
    print("-" * 70)

    # Guardar copia física en data/gold/fact_energia_dia.csv
    gold_csv = os.path.join("data", "gold", "fact_energia_dia.csv")
    os.makedirs(os.path.dirname(gold_csv), exist_ok=True)
    with open(gold_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["fecha", "dispositivo_id", "energia_kwh", "pct_datos_validos", "filas_validas", "filas_esperadas"])
        for r in dwh_filas:
            writer.writerow([r["fecha"], r["dispositivo_id"], r["energia_kwh"], r["pct_datos_validos"], r["filas_validas"], r["filas_esperadas"]])
    print(f"[OK] Archivo Gold físico guardado en: {gold_csv}")

    # -------------------------------------------------------------------------
    # PASO 4: CARGA IDEMPOTENTE (UPSERT) EN POSTGRESQL
    # -------------------------------------------------------------------------
    # Generar script SQL idempotente de respaldo y portabilidad
    os.makedirs(os.path.dirname(SQL_OUTPUT), exist_ok=True)
    with open(SQL_OUTPUT, "w", encoding="utf-8") as f:
        f.write("-- Carga Idempotente generada por etl/run_etl.py\n")
        f.write("BEGIN;\n\n")
        
        f.write("-- 1. UPSERT en silver.lectura_5min\n")
        for r in filas_validas:
            f.write(
                f"INSERT INTO silver.lectura_5min (ts, dispositivo_id, p_ac_kw, irradiancia_wm2, temp_modulo_c)\n"
                f"VALUES ('{r['ts']}', {r['dispositivo_id']}, {r['p_ac_kw']}, {r['irradiancia_wm2']}, {r['temp_modulo_c']})\n"
                f"ON CONFLICT (ts, dispositivo_id) DO UPDATE SET\n"
                f"    p_ac_kw = EXCLUDED.p_ac_kw,\n"
                f"    irradiancia_wm2 = EXCLUDED.irradiancia_wm2,\n"
                f"    temp_modulo_c = EXCLUDED.temp_modulo_c;\n"
            )
        
        f.write("\n-- 2. UPSERT en dwh.fact_energia_dia\n")
        for r in dwh_filas:
            f.write(
                f"INSERT INTO dwh.fact_energia_dia (fecha, dispositivo_id, energia_kwh, pct_datos_validos, filas_validas, filas_esperadas, updated_at)\n"
                f"VALUES ('{r['fecha']}', {r['dispositivo_id']}, {r['energia_kwh']}, {r['pct_datos_validos']}, {r['filas_validas']}, {r['filas_esperadas']}, CURRENT_TIMESTAMP)\n"
                f"ON CONFLICT (fecha, dispositivo_id) DO UPDATE SET\n"
                f"    energia_kwh = EXCLUDED.energia_kwh,\n"
                f"    pct_datos_validos = EXCLUDED.pct_datos_validos,\n"
                f"    filas_validas = EXCLUDED.filas_validas,\n"
                f"    filas_esperadas = EXCLUDED.filas_esperadas,\n"
                f"    updated_at = CURRENT_TIMESTAMP;\n"
            )
        f.write("\nCOMMIT;\n")

    print(f"[OK] Script de carga SQL generado en: {SQL_OUTPUT}")

    # Intentar conexión directa a PostgreSQL si el driver está disponible
    db_conectada = False
    try:
        import psycopg2
        print(f"\n[BD] Conectando a PostgreSQL ({PG_HOST}:{PG_PORT}/{PG_DB})...")
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD
        )
        cur = conn.cursor()

        # Ejecutar DDL base si es necesario
        with open(os.path.join("sql", "schema.sql"), "r", encoding="utf-8") as schema_file:
            cur.execute(schema_file.read())

        # Ejecutar UPSERT Silver
        for r in filas_validas:
            cur.execute("""
                INSERT INTO silver.lectura_5min (ts, dispositivo_id, p_ac_kw, irradiancia_wm2, temp_modulo_c)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (ts, dispositivo_id) DO UPDATE SET
                    p_ac_kw = EXCLUDED.p_ac_kw,
                    irradiancia_wm2 = EXCLUDED.irradiancia_wm2,
                    temp_modulo_c = EXCLUDED.temp_modulo_c;
            """, (r["ts"], r["dispositivo_id"], r["p_ac_kw"], r["irradiancia_wm2"], r["temp_modulo_c"]))

        # Ejecutar UPSERT DWH
        for r in dwh_filas:
            cur.execute("""
                INSERT INTO dwh.fact_energia_dia (fecha, dispositivo_id, energia_kwh, pct_datos_validos, filas_validas, filas_esperadas, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (fecha, dispositivo_id) DO UPDATE SET
                    energia_kwh = EXCLUDED.energia_kwh,
                    pct_datos_validos = EXCLUDED.pct_datos_validos,
                    filas_validas = EXCLUDED.filas_validas,
                    filas_esperadas = EXCLUDED.filas_esperadas,
                    updated_at = CURRENT_TIMESTAMP;
            """, (r["fecha"], r["dispositivo_id"], r["energia_kwh"], r["pct_datos_validos"], r["filas_validas"], r["filas_esperadas"]))

        conn.commit()

        # Auditoría de conteos para demostrar idempotencia
        cur.execute("SELECT COUNT(*) FROM silver.lectura_5min;")
        count_silver = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*), SUM(energia_kwh) FROM dwh.fact_energia_dia;")
        count_dwh, suma_energia = cur.fetchone()

        print("\n" + "=" * 70)
        print("ESTADO EN POSTGRESQL TRAS CARGA (DEMOSTRACIÓN DE IDEMPOTENCIA):")
        print("=" * 70)
        print(f"  • Filas en 'silver.lectura_5min':   {count_silver}")
        print(f"  • Filas en 'dwh.fact_energia_dia':  {count_dwh}")
        print(f"  • Energía total acumulada:          {suma_energia:.3f} kWh")
        print("=" * 70)
        print("[OK] Carga en PostgreSQL completada exitosamente sin duplicación.")

        cur.close()
        conn.close()
        db_conectada = True

    except ImportError:
        print("[INFO] 'psycopg2' no está instalado en el entorno Python local.")
        print("       Las sentencias de carga idempotente se guardaron listas en 'sql/carga_idempotente.sql'")
        print("       Para ejecutar directamente en BD: pip install psycopg2-binary")
    except Exception as e:
        print(f"[WARN] No se pudo conectar a PostgreSQL en {PG_HOST}:{PG_PORT}: {e}")
        print("       El pipeline generó exitosamente los datos Silver y el archivo 'sql/carga_idempotente.sql'")
        print("       para ser ejecutado en PostgreSQL vía DBeaver, pgAdmin o psql.")

    print("\n" + "=" * 70)
    print(">>> PIPELINE ETL COMPLETADO CON ÉXITO <<<")
    print("=" * 70)


if __name__ == "__main__":
    ejecutar_etl()
