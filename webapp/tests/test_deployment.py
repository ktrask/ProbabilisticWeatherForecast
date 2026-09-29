"""Offline tests for how the app is served: the container's entry point and image.

Nothing here starts a server; these pin the configuration decisions that decide
what runs and what is exposed, because getting them wrong is not visible from
the outside until it is. The app itself is read-only by construction - see
test_api.TestContract.test_every_route_is_read_only.
"""
from pathlib import Path

import pytest


class TestContainerEntryPoint:
    @staticmethod
    @pytest.fixture(scope="class")
    def startup():
        """The executable lines only - the comments describe what was replaced,
        so matching against them would defeat the point."""
        return "\n".join(
            line for line in open("startup.sh").read().splitlines()
            if line.strip() and not line.strip().startswith("#")
        )

    def test_uses_a_production_server(self, startup):
        assert "gunicorn" in startup
        assert "run.py" not in startup, "a development server must not serve traffic"

    def test_execs_so_signals_reach_it(self, startup):
        """Without exec, gunicorn is a child of bash and docker stop has to
        escalate to SIGKILL."""
        assert "exec gunicorn" in startup

    def test_does_not_restart_in_a_loop(self, startup):
        """The old loop restarted after every crash, hiding the failure."""
        assert "while" not in startup

    def test_fails_fast(self, startup):
        assert "set -euo pipefail" in startup

    def test_gunicorn_is_a_declared_dependency(self):
        assert "gunicorn" in open("requirements.txt").read()

    def test_timeout_survives_a_slow_forecast(self, startup):
        """A cold Open-Meteo fetch waits up to 30 s by itself - gunicorn's own
        default, which would kill the worker mid-request."""
        assert "--timeout" in startup

    def test_container_does_not_run_as_root(self):
        dockerfile = open("Dockerfile").read()
        assert "USER meteogram" in dockerfile

    def test_no_secret_is_baked_into_the_image(self):
        dockerfile = open("Dockerfile").read()
        assert "ENV SECRET_KEY" not in dockerfile


class TestContainerServesTheNewApp:
    """The switch-over: the container runs the FastAPI app, which also serves
    the built frontend and the pictograms. The Flask app is no longer in it."""

    @staticmethod
    @pytest.fixture(scope="class")
    def dockerfile():
        return "\n".join(
            line for line in open("Dockerfile").read().splitlines()
            if line.strip() and not line.strip().startswith("#")
        )

    @staticmethod
    @pytest.fixture(scope="class")
    def startup():
        return "\n".join(
            line for line in open("startup.sh").read().splitlines()
            if line.strip() and not line.strip().startswith("#")
        )

    def test_gunicorn_runs_the_app_factory_on_uvicorn_workers(self, startup):
        assert '"api.app:create_app()"' in startup
        # uvicorn.workers is deprecated; the separate package replaces it.
        assert "--worker-class uvicorn_worker.UvicornWorker" in startup
        assert "uvicorn-worker" in open("requirements.txt").read()

    def test_the_frontend_is_built_in_its_own_stage(self, dockerfile):
        assert "FROM node:22-slim AS frontend" in dockerfile
        assert "COPY --from=frontend /build/dist /app/frontend/dist" in dockerfile

    def test_the_built_frontend_lands_where_the_app_looks(self, dockerfile):
        from api.settings import DEFAULT_FRONTEND

        assert DEFAULT_FRONTEND.relative_to(Path.cwd()).as_posix() == "frontend/dist"

    @pytest.mark.parametrize("package", ["core", "sources", "vsup", "api", "config"])
    def test_every_part_of_the_backend_is_copied(self, dockerfile, package):
        assert f"COPY {package} /app/{package}" in dockerfile

    def test_the_pictograms_are_copied_where_vsup_yaml_points(self, dockerfile):
        """If the pictograms move, the image must follow, or every pictogram
        is a 404 in production only."""
        from vsup.config import load

        root = load().pictogram_root.relative_to(Path.cwd()).as_posix()
        assert f"COPY {root} /app/{root}" in dockerfile

    def test_the_offline_fixtures_are_where_the_offline_config_points(self, dockerfile):
        assert "COPY tests/fixtures /app/tests/fixtures" in dockerfile
        assert "directory: ../tests/fixtures" in open("config/sources.fixtures.yaml").read()

    def test_the_flask_app_is_not_in_the_image(self, dockerfile):
        for legacy in ("COPY app ", "run.py", "config.py"):
            assert legacy not in dockerfile, f"{legacy!r} is still copied"

    def test_it_reports_its_health(self, dockerfile):
        assert "HEALTHCHECK" in dockerfile and "/api/health" in dockerfile
