"""Excepciones explícitas del pipeline.

Separar los errores por tipo permite que los DAGs decidan con criterio:
* errores *reintentables* (límite de tasa, caída del servidor) -> Airflow reintenta;
* errores *definitivos* (credenciales, contrato roto, calidad pésima) -> se falla
  de inmediato con un mensaje claro, sin gastar reintentos.
"""


class ErrorPipelineDeporte(Exception):
    """Base de todos los errores del pipeline."""


class ErrorReintentable(ErrorPipelineDeporte):
    """El fallo es transitorio: vale la pena reintentar."""


class ErrorDefinitivo(ErrorPipelineDeporte):
    """El fallo no se arregla reintentando: requiere intervención humana."""


class ErrorCredenciales(ErrorDefinitivo):
    """La API rechazó el token (401/403). Revisar la Connection en Airflow."""


class ErrorPeticionInvalida(ErrorDefinitivo):
    """La API respondió 400/404: la petición está mal construida."""


class ErrorEsquemaApi(ErrorDefinitivo):
    """La respuesta de la API no respeta el contrato esperado."""


class ErrorLimiteTasa(ErrorReintentable):
    """Se agotaron los reintentos internos ante respuestas 429."""


class ErrorServidorApi(ErrorReintentable):
    """La API respondió 5xx."""


class ErrorCalidadDatos(ErrorDefinitivo):
    """Un archivo trae demasiadas filas inválidas como para cargarlo."""


class ErrorConfiguracion(ErrorDefinitivo):
    """La Variable de configuración del pipeline es inválida."""
