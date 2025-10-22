"""Filtra filas de un archivo de Excel por códigos de organismo público.

Este script permite cargar un archivo de Excel y conservar únicamente las filas cuya
columna ``CodigoOrganismoPublico`` contenga alguno de los códigos permitidos y elimina
filas duplicadas según la columna ``Codigo``.

Uso:
    python filtrar_organismos.py --input archivo.xlsx

El archivo generado se guardará junto al original con el prefijo ``Fil`` y no es
configurable.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

import pandas as pd
from tqdm import tqdm


ALLOWED_CODES: Sequence[str] = (
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
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Filtra un archivo de Excel conservando solo las filas cuyo "
            "CodigoOrganismoPublico esté en la lista de códigos permitidos "
            "y elimina duplicados según la columna Codigo."
        )
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Ruta del archivo de Excel a procesar",
    )
    parser.add_argument(
        "--sheet",
        "-s",
        default=0,
        help=(
            "Nombre o índice de la hoja a leer. Por defecto se usa la primera hoja."
        ),
    )
    return parser.parse_args()


def filter_excel(input_path: Path, output_path: Path, sheet: int | str = 0) -> None:
    df = pd.read_excel(
        input_path,
        sheet_name=sheet,
        dtype={"CodigoOrganismoPublico": str, "Codigo": str},
    )
    if "CodigoOrganismoPublico" not in df.columns:
        raise ValueError(
            "La hoja seleccionada no contiene la columna 'CodigoOrganismoPublico'."
        )
    if "Codigo" not in df.columns:
        raise ValueError("La hoja seleccionada no contiene la columna 'Codigo'.")

    tqdm.pandas(desc="Filtrando filas")

    def is_allowed(code: object) -> bool:
        if pd.isna(code):
            return False
        return str(code) in ALLOWED_CODES

    mask = df["CodigoOrganismoPublico"].progress_apply(is_allowed)
    filtered_df = df[mask].copy()
    deduped_df = filtered_df.drop_duplicates(subset=["Codigo"], keep="first")
    deduped_df.to_excel(output_path, index=False)


def main() -> None:
    args = parse_args()
    input_path = Path(args.input).expanduser().resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"El archivo de entrada no existe: {input_path}")

    output_path = input_path.parent / f"Fil{input_path.name}"

    filter_excel(input_path, output_path, sheet=args.sheet)


if __name__ == "__main__":
    main()
