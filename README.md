# Consulta de Órdenes de Compra de Mercado Público

## Requisitos previos
- Python 3.10 o superior.
- Dependencias de Python:
  - `requests`
  - `python-dateutil`
  - `pandas`
  - `openpyxl`
  - `tqdm` (opcional, para barra de progreso)

Instala las dependencias en un entorno virtual (recomendado):

```bash
python -m venv .venv
source .venv/bin/activate  # En Windows usa: .venv\Scripts\activate
pip install requests python-dateutil pandas openpyxl tqdm
```

## Uso
Dentro del script debes configurar los ID de los servicios públicos a consultar en la línea 21 del código:
```bash
ORGANISMOS = [
(Código de organismos entrecomillas y separado por comas en caso de que sea más de uno; Ejemplo:
 "1675210", "1820906" 
 buscar en: https://datos-abiertos.chilecompra.cl/descargas/complementos -> "Registro histórico de organismos compradores")
]
```
Nota: Por defecto el Script contiene el código de 26 organismos locales de educación pública (líneas 22 a 60), eliminar y cambiar por el requerido.

Ejecuta el script `consulta_api.py` desde la terminal indicando el rango de fechas y tu ticket/token de Mercado Público:

```bash
python consulta_api.py --desde 01-09-2025 --hasta 02-09-2025 --ticket TU_TOKEN
```

### Parámetros obligatorios
- `--desde`: fecha inicial (formato `dd-mm-YYYY`).
- `--hasta`: fecha final (formato `dd-mm-YYYY`).
- `--ticket`: token de acceso (no puede estar vacío).

### Parámetros opcionales
- `--timeout`: tiempo máximo de lectura en segundos (por defecto `120`).
- `--sleep`: pausa entre consultas de listado en segundos (por defecto `0.20`).
- `--sleep-detail`: pausa entre consultas de detalle en segundos (por defecto `0.22`).
- `--progress-every`: frecuencia de logs cuando no está disponible `tqdm` (por defecto `100`).
- `--batch-size`: tamaño de lote para escritura en CSV (por defecto `1000`).
- `--retries`: cantidad de reintentos por petición (por defecto `5`).
- `--workers`: cantidad de hilos para paralelizar las consultas (por defecto `min(8, cpu_count)`; usa `1` para modo secuencial).

## Salida
El script generará en el directorio actual:
- `consulta_api.csv`
- `consulta_api.xlsx` (hoja "Órdenes de Compra")
- `log_api.txt`
- `log_api`

Cada fila representa una orden de compra cuyo campo `Fechas.FechaCreacion` se encuentre dentro del rango solicitado. Las columnas aparecen en el orden requerido por la especificación.
