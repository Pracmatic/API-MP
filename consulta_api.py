import argparse
import csv
import logging
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from threading import Lock, local

import pandas as pd
import requests
from dateutil import parser as date_parser

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover
    tqdm = None


BASE_URL = "https://api.mercadopublico.cl/servicios/v1/publico/ordenesdecompra.json"
ORGANISMOS = [
    "1675210",
    "1820906",
    "1820922",
    "1723291",
    "1622793",
    "1593366",
    "1722523",
    "1593370",
    "1675231",
    "1890634",
    "1890637",
    "1890640",
    "1890619",
    "1717135",
    "1820918",
    "1820915",
    "1675218",
    "1890597",
    "1622818",
    "1890653",
    "1717137",
    "1975701",
    "1820914",
    "1820911",
    "1890644",
    "1890625",
    "1890618",
    "1890635",
    "1959810",
    "1959820",
    "1959828",
    "1959831",
    "1959829",
    "1959833",
    "1972213",
    "1959834",
    "1959832",
    "1959836",
    "7271",
]

COLUMNAS = [
    "Código OC",
    "Nombre",
    "Código Estado",
    "Descripción",
    "Código Licitación",
    "Tipo",
    "Moneda",
    "Fecha Creación",
    "Fecha Envío",
    "Fecha Aceptación",
    "Total",
    "Total Neto",
    "Proveedor",
    "Rut Proveedor",
    "Unidad Compradora",
    "Nombre Organismo",
    "Contacto Comprador",
    "Fuente Financiamiento",
    "Codigo categoria",
    "Categoría",
    "Codigo producto",
]


_THREAD_LOCAL = local()
_SESSIONS: list[requests.Session] = []
_SESSIONS_LOCK = Lock()


def _get_thread_session() -> requests.Session:
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        _THREAD_LOCAL.session = session
        with _SESSIONS_LOCK:
            _SESSIONS.append(session)
    return session


def _close_thread_sessions():
    with _SESSIONS_LOCK:
        while _SESSIONS:
            session = _SESSIONS.pop()
            session.close()


def parse_fecha_arg(valor: str) -> date:
    try:
        return datetime.strptime(valor, "%d-%m-%Y").date()
    except ValueError as exc:  # pragma: no cover
        raise argparse.ArgumentTypeError(f"Formato de fecha inválido: {valor}") from exc


def parse_ticket(valor: str) -> str:
    token = valor.strip()
    if not token:
        raise argparse.ArgumentTypeError("El ticket/token no puede estar vacío")
    return token


def parse_workers(valor: str) -> int:
    try:
        workers = int(valor)
    except ValueError as exc:  # pragma: no cover
        raise argparse.ArgumentTypeError("La cantidad de hilos debe ser un entero") from exc
    if workers < 1:
        raise argparse.ArgumentTypeError("La cantidad de hilos debe ser al menos 1")
    return workers


def rango_fechas(desde: date, hasta: date):
    actual = desde
    delta = timedelta(days=1)
    while actual <= hasta:
        yield actual
        actual += delta


def to_api_date(dia: date) -> str:
    return dia.strftime("%d%m%Y")


def parse_fecha_json(valor) -> date | None:
    if not valor:
        return None
    try:
        return date_parser.parse(valor).date()
    except (ValueError, TypeError, OverflowError):  # pragma: no cover
        return None


def formatear_fecha_salida(valor) -> str:
    fecha = parse_fecha_json(valor)
    return fecha.strftime("%d-%m-%Y") if fecha else ""


def safe_get(data, *keys):
    actual = data
    for key in keys:
        if isinstance(actual, dict) and key in actual:
            actual = actual[key]
        else:
            return ""
    return actual if actual is not None else ""


def normalizar_texto(valor) -> str:
    if isinstance(valor, (list, tuple, set)):
        for elemento in valor:
            resultado = normalizar_texto(elemento)
            if resultado:
                return resultado
        return ""
    if isinstance(valor, dict):
        for campo in ("Nombre", "Descripcion", "DescripcionLarga", "DescripcionCorta", "Glosa", "Valor", "Codigo"):
            resultado = valor.get(campo)
            if resultado:
                return str(resultado)
        return ""
    if valor is None:
        return ""
    return str(valor)


def normalizar_moneda(valor) -> str:
    if isinstance(valor, list):
        for elemento in valor:
            resultado = normalizar_moneda(elemento)
            if resultado:
                return resultado
        return ""
    if isinstance(valor, dict):
        for campo in ("Nombre", "Descripcion", "DescripcionMoneda", "Codigo", "CodigoMoneda", "Moneda"):
            resultado = valor.get(campo)
            if resultado:
                return resultado
        return ""
    return valor or ""


def obtener_moneda(oc: dict, primer_item: dict | None) -> str:
    moneda = normalizar_moneda(oc.get("Moneda"))
    if moneda:
        return moneda
    if primer_item:
        return normalizar_moneda(primer_item.get("Moneda"))
    return ""


def obtener_fuente_financiamiento(oc: dict) -> str:
    for campo in ("FuenteFinanciamiento", "Financiamiento"):
        fuente = normalizar_texto(oc.get(campo))
        if fuente:
            return fuente
    return normalizar_texto(safe_get(oc, "Comprador", "FuenteFinanciamiento"))


def obtener_rut_proveedor(oc: dict) -> str:
    rut = normalizar_texto(safe_get(oc, "Proveedor", "RutProveedor"))
    if rut:
        return rut
    return normalizar_texto(safe_get(oc, "Proveedor", "RutSucursal"))


def configurar_logger() -> logging.Logger:
    logger = logging.getLogger("consulta_api")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    consola = logging.StreamHandler(sys.stdout)
    consola.setFormatter(formatter)
    logger.addHandler(consola)

    archivo = logging.FileHandler("log_api.txt", encoding="utf-8")
    archivo.setFormatter(formatter)
    logger.addHandler(archivo)

    return logger


def request_with_retries(session: requests.Session, params: dict, read_timeout: float, retries: int, logger: logging.Logger):
    attempt = 0
    wait = 1.0
    while attempt < retries:
        try:
            response = session.get(BASE_URL, params=params, timeout=(10, read_timeout))
            if response.status_code == 200:
                return response
            if response.status_code in {429} or 500 <= response.status_code < 600:
                logger.warning(
                    "Respuesta %s para params %s. Reintentando en %.1fs", response.status_code, params, wait
                )
            else:
                logger.error("Error %s para params %s", response.status_code, params)
                return None
        except requests.RequestException as exc:
            logger.warning("Excepción en request (%s). Reintentando en %.1fs", exc, wait)
        attempt += 1
        time.sleep(wait)
        wait *= 2
    logger.error("Agotados los reintentos para params %s", params)
    return None


def listar_oc_por_rango(ticket, organismos, desde_dt, hasta_dt, args, logger):
    codigos: list[str] = []
    vistos = set()
    fechas = list(rango_fechas(desde_dt, hasta_dt))
    combinaciones = [(dia, org) for dia in fechas for org in organismos]
    total = len(combinaciones)

    if args.workers <= 1:
        session = requests.Session()
        try:
            if tqdm:
                iterable = tqdm(combinaciones, total=total, desc="Listando", unit="consulta")
            else:
                iterable = combinaciones
            for idx, (dia, organismo) in enumerate(iterable, start=1):
                params = {"fecha": to_api_date(dia), "CodigoOrganismo": organismo, "ticket": ticket}
                response = request_with_retries(session, params, args.timeout, args.retries, logger)
                if response is None:
                    continue
                try:
                    payload = response.json()
                except ValueError:  # pragma: no cover
                    logger.error("Respuesta no es JSON para params %s", params)
                    continue
                listado = payload.get("Listado") or []
                for registro in listado:
                    codigo = registro.get("Codigo") or registro.get("codigo")
                    if codigo and codigo not in vistos:
                        vistos.add(codigo)
                        codigos.append(codigo)
                if not tqdm and idx % args.progress_every == 0:
                    logger.info("Procesados %s de %s combinaciones", idx, total)
                time.sleep(args.sleep)
            return codigos
        finally:
            session.close()

    lock = Lock()

    def procesar(dia: date, organismo: str) -> list[str]:
        session = _get_thread_session()
        params = {"fecha": to_api_date(dia), "CodigoOrganismo": organismo, "ticket": ticket}
        response = request_with_retries(session, params, args.timeout, args.retries, logger)
        resultados: list[str] = []
        if response is not None:
            try:
                payload = response.json()
            except ValueError:  # pragma: no cover
                logger.error("Respuesta no es JSON para params %s", params)
            else:
                listado = payload.get("Listado") or []
                for registro in listado:
                    codigo = registro.get("Codigo") or registro.get("codigo")
                    if codigo:
                        resultados.append(codigo)
        time.sleep(args.sleep)
        return resultados

    progress = tqdm(total=total, desc="Listando", unit="consulta") if tqdm else None
    try:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_combo = {
                executor.submit(procesar, dia, organismo): (dia, organismo) for dia, organismo in combinaciones
            }
            for idx, future in enumerate(as_completed(future_to_combo), start=1):
                dia, organismo = future_to_combo[future]
                try:
                    nuevos = future.result()
                except Exception as exc:  # pragma: no cover
                    logger.exception("Error listando combinación (%s, %s): %s", dia, organismo, exc)
                    nuevos = []
                if nuevos:
                    with lock:
                        for codigo in nuevos:
                            if codigo not in vistos:
                                vistos.add(codigo)
                                codigos.append(codigo)
                if progress:
                    progress.update(1)
                elif idx % args.progress_every == 0:
                    logger.info("Procesados %s de %s combinaciones", idx, total)
    finally:
        if progress:
            progress.close()
        _close_thread_sessions()
    return codigos


def construir_fila_oc(oc: dict) -> dict:
    fechas = oc.get("Fechas") or {}
    items = oc.get("Items") or {}
    if isinstance(items, dict):
        listado_items = items.get("Listado") or items.get("Item") or []
    else:
        listado_items = items or []
    if isinstance(listado_items, dict):
        listado_items = [listado_items]
    primer_item = listado_items[0] if listado_items else {}

    fila = {
        "Código OC": oc.get("Codigo", ""),
        "Nombre": oc.get("Nombre", ""),
        "Código Estado": oc.get("CodigoEstado", ""),
        "Descripción": oc.get("Descripcion", ""),
        "Código Licitación": oc.get("CodigoLicitacion", ""),
        "Tipo": oc.get("Tipo", ""),
        "Moneda": obtener_moneda(oc, primer_item),
        "Fecha Creación": formatear_fecha_salida(fechas.get("FechaCreacion")),
        "Fecha Envío": formatear_fecha_salida(fechas.get("FechaEnvio")),
        "Fecha Aceptación": formatear_fecha_salida(fechas.get("FechaAceptacion")),
        "Total": oc.get("Total", ""),
        "Total Neto": oc.get("TotalNeto", ""),
        "Proveedor": safe_get(oc, "Proveedor", "Nombre"),
        "Rut Proveedor": obtener_rut_proveedor(oc),
        "Unidad Compradora": safe_get(oc, "Comprador", "NombreUnidad"),
        "Nombre Organismo": safe_get(oc, "Comprador", "NombreOrganismo"),
        "Contacto Comprador": safe_get(oc, "Comprador", "Nombre"),
        "Fuente Financiamiento": obtener_fuente_financiamiento(oc),
        "Codigo categoria": primer_item.get("CodigoCategoria", "") if primer_item else "",
        "Categoría": primer_item.get("Categoria", "") if primer_item else "",
        "Codigo producto": primer_item.get("CodigoProducto", "") if primer_item else "",
    }
    for columna in COLUMNAS:
        fila.setdefault(columna, "")
    return fila


def descargar_detalle_y_escribir(ticket, codigos, args, desde_dt, hasta_dt, logger):
    csv_path = Path("consulta_api.csv")
    escribir_header = True
    filas_batch = []
    escritos = 0

    if csv_path.exists():
        csv_path.unlink()

    def flush():
        nonlocal escribir_header, filas_batch, escritos
        if not filas_batch:
            return
        with csv_path.open("a", newline="", encoding="utf-8") as archivo:
            writer = csv.DictWriter(archivo, fieldnames=COLUMNAS)
            if escribir_header:
                writer.writeheader()
                escribir_header = False
            writer.writerows(filas_batch)
        escritos += len(filas_batch)
        filas_batch = []

    if args.workers <= 1:
        session = requests.Session()
        try:
            iterable = tqdm(codigos, desc="Descargando", unit="oc") if tqdm else codigos
            for idx, codigo in enumerate(iterable, start=1):
                params = {"codigo": codigo, "ticket": ticket}
                response = request_with_retries(session, params, args.timeout, args.retries, logger)
                if response is None:
                    continue
                try:
                    payload = response.json()
                except ValueError:  # pragma: no cover
                    logger.error("Detalle no es JSON para código %s", codigo)
                    continue
                listado = payload.get("Listado") or []
                if isinstance(listado, dict):
                    listado = [listado]
                if not listado:
                    continue
                oc = listado[0]
                fecha_creacion = parse_fecha_json(safe_get(oc, "Fechas", "FechaCreacion"))
                if fecha_creacion is None or fecha_creacion < desde_dt or fecha_creacion > hasta_dt:
                    continue
                fila = construir_fila_oc(oc)
                filas_batch.append(fila)
                if len(filas_batch) >= args.batch_size:
                    flush()
                if not tqdm and idx % args.progress_every == 0:
                    logger.info("Procesados %s de %s códigos", idx, len(codigos))
                pausa = args.sleep_detail + random.uniform(0, max(args.sleep_detail * 0.1, 0.01))
                time.sleep(pausa)
        finally:
            session.close()
    else:
        total = len(codigos)
        progress = tqdm(total=total, desc="Descargando", unit="oc") if tqdm else None

        def procesar(codigo: str):
            session_local = _get_thread_session()
            params = {"codigo": codigo, "ticket": ticket}
            pausa = args.sleep_detail + random.uniform(0, max(args.sleep_detail * 0.1, 0.01))
            try:
                response = request_with_retries(session_local, params, args.timeout, args.retries, logger)
                if response is None:
                    return None
                try:
                    payload = response.json()
                except ValueError:  # pragma: no cover
                    logger.error("Detalle no es JSON para código %s", codigo)
                    return None
                listado = payload.get("Listado") or []
                if isinstance(listado, dict):
                    listado = [listado]
                if not listado:
                    return None
                oc = listado[0]
                fecha_creacion = parse_fecha_json(safe_get(oc, "Fechas", "FechaCreacion"))
                if fecha_creacion is None or fecha_creacion < desde_dt or fecha_creacion > hasta_dt:
                    return None
                return construir_fila_oc(oc)
            finally:
                time.sleep(pausa)

        try:
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                future_to_codigo = {executor.submit(procesar, codigo): codigo for codigo in codigos}
                for idx, future in enumerate(as_completed(future_to_codigo), start=1):
                    codigo = future_to_codigo[future]
                    try:
                        fila = future.result()
                    except Exception as exc:  # pragma: no cover
                        logger.exception("Error descargando código %s: %s", codigo, exc)
                        fila = None
                    if fila:
                        filas_batch.append(fila)
                        if len(filas_batch) >= args.batch_size:
                            flush()
                    if progress:
                        progress.update(1)
                    elif idx % args.progress_every == 0:
                        logger.info("Procesados %s de %s códigos", idx, total)
        finally:
            if progress:
                progress.close()
            _close_thread_sessions()

    flush()
    logger.info("Total de órdenes escritas: %s", escritos)
    return csv_path


def generar_excel_desde_csv(csv_path: Path, xlsx_path: Path, columnas_orden):
    df = pd.read_csv(csv_path, dtype=str).fillna("")
    for columna in columnas_orden:
        if columna not in df.columns:
            df[columna] = ""
    df = df[columnas_orden]
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Órdenes de Compra")


def duplicar_log():
    origen = Path("log_api.txt")
    destino = Path("log_api")
    if origen.exists():
        destino.write_bytes(origen.read_bytes())


def parse_args():
    parser = argparse.ArgumentParser(
        description="Consulta la API de Mercado Público",
        allow_abbrev=False,
    )
    parser.add_argument("--desde", required=True, type=parse_fecha_arg)
    parser.add_argument("--hasta", required=True, type=parse_fecha_arg)
    parser.add_argument("--ticket", required=True, type=parse_ticket)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--sleep", type=float, default=0.20)
    parser.add_argument("--sleep-detail", dest="sleep_detail", type=float, default=0.22)
    parser.add_argument("--progress-every", dest="progress_every", type=int, default=100)
    parser.add_argument("--batch-size", dest="batch_size", type=int, default=1000)
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument(
        "--workers",
        type=parse_workers,
        default=max(1, min(8, (os.cpu_count() or 4))),
        help="Cantidad de hilos para peticiones concurrentes (1 para modo secuencial)",
    )
    return parser.parse_args()


def validar_rango(desde: date, hasta: date):
    if hasta < desde:
        raise ValueError("La fecha hasta no puede ser anterior a la fecha desde")


def main():
    args = parse_args()
    try:
        validar_rango(args.desde, args.hasta)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    logger = configurar_logger()
    logger.info("Inicio de consulta desde %s hasta %s", args.desde, args.hasta)

    try:
        codigos = listar_oc_por_rango(args.ticket, ORGANISMOS, args.desde, args.hasta, args, logger)
        logger.info("Total de códigos únicos obtenidos: %s", len(codigos))

        csv_path = descargar_detalle_y_escribir(args.ticket, codigos, args, args.desde, args.hasta, logger)
        if csv_path.exists():
            generar_excel_desde_csv(csv_path, Path("consulta_api.xlsx"), COLUMNAS)
            logger.info("Archivos generados: %s y consulta_api.xlsx", csv_path.name)
        else:
            logger.warning("No se generó archivo CSV")
    except Exception as exc:  # pragma: no cover
        logger.exception("Error inesperado: %s", exc)
        duplicar_log()
        return 1

    logger.info("Proceso finalizado")
    duplicar_log()
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
