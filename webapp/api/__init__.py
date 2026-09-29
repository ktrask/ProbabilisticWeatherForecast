"""The HTTP API of the new backend: JSON and static pictograms, nothing rendered.

    python -m api serve                 # development server, 127.0.0.1:8000
    python -m api openapi -o api/openapi.json

`app.create_app()` builds the application. Both config files are loaded and
cross-checked there, so a broken config stops the start rather than failing
requests. Every route is a read-only GET.
"""
