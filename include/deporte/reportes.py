"""Capa de consumo: reporte semanal para el cuerpo técnico y la gerencia."""

from __future__ import annotations

RESULTADOS = {"G": "Victoria", "E": "Empate", "P": "Derrota"}


def _soles(valor) -> str:
    return "-" if valor is None else f"S/ {float(valor):,.0f}"


def generar_reporte_markdown(fecha: str, partidos: list[dict], riesgo: list[dict]) -> str:
    """Arma un reporte legible a partir de las filas de los marts de dbt."""
    lineas = [f"# Reporte DeporData — Club Deportivo SQ ({fecha})", ""]

    lineas += ["## Últimos partidos", ""]
    if partidos:
        lineas += [
            "| Fecha | Competición | Rival | Condición | Marcador | Resultado | Carga 7d previa (UA) | Recaudación |",
            "|---|---|---|---|---|---|---|---|",
        ]
        for p in partidos:
            lineas.append(
                f"| {p['FECHA_PARTIDO']} | {p['COMPETICION']} | {p['RIVAL']} | "
                f"{'Local' if p['ES_LOCAL'] else 'Visita'} | {p['GOLES_FAVOR']}-{p['GOLES_CONTRA']} | "
                f"{RESULTADOS.get(p['RESULTADO'], p['RESULTADO'])} | "
                f"{p['CARGA_EQUIPO_7D_PREVIA'] or '-'} | {_soles(p['RECAUDACION_NETA'])} |"
            )
        puntos = sum(int(p["PUNTOS"]) for p in partidos)
        lineas += ["", f"Puntos obtenidos en el período: **{puntos} de {3 * len(partidos)}**."]
    else:
        lineas.append("Sin partidos finalizados en el período.")

    lineas += ["", "## Jugadores en riesgo de lesión (ACWR)", ""]
    alertas = [r for r in riesgo if r["NIVEL_RIESGO"] in ("ALTO", "BAJA_CARGA")]
    if alertas:
        lineas += ["| Jugador | Posición | ACWR | Nivel |", "|---|---|---|---|"]
        for r in alertas:
            lineas.append(f"| {r['NOMBRE']} | {r['POSICION']} | {float(r['ACWR']):.2f} | {r['NIVEL_RIESGO']} |")
    else:
        lineas.append("Ningún jugador fuera de la zona óptima (0.8 – 1.3).")
    lineas.append("")
    return "\n".join(lineas)
