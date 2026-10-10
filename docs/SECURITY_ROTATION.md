# Rotación de secretos (A0)

El archivo `docs/centraal_dataframework.yaml` contenía una URL de Logic App con
`sig` y correos de clientes. Ese contenido fue reemplazado por un ejemplo ficticio.

## Acciones obligatorias en Azure

1. Regenerar / rotar la clave (`sig`) de la Logic App de notificaciones.
2. Configurar `LOGICAPP_EMAIL_URL` en Application Settings (preferible vía Key Vault reference).
3. Restringir en la Logic App los destinatarios a una allowlist.

## Limpieza de historial git (coordinar con el equipo)

Esto reescribe el historial. Ejecutar solo tras acuerdo:

```bash
pip install git-filter-repo
git filter-repo --path docs/centraal_dataframework.yaml --invert-paths
# o reemplazar el blob histórico con el ejemplo sanitizado
git push --force --all
git push --force --tags
```

Todos los clones locales deben re-clonar o resetear tras el force-push.
