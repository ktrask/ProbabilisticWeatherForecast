"""Offline tests for how the app is served.

Nothing here starts a server; these pin the configuration decisions that decide
what is exposed, because getting them wrong is not visible from the outside until
it is being exploited.
"""
import importlib
import os

import pytest

import config
import run


class TestDebugServer:
    """debug + a public bind turns the Werkzeug debugger into remote code
    execution for anyone who can reach the port and provoke a traceback."""

    def test_debug_is_off_by_default(self):
        host, port, debug = run.serverOptions({})
        assert debug is False

    def test_binds_loopback_by_default(self):
        host, port, debug = run.serverOptions({})
        assert host in run.LOOPBACK, "the dev server should not be public by default"
        assert port == run.DEFAULT_PORT

    @pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on"])
    def test_debug_can_be_turned_on(self, value):
        _, _, debug = run.serverOptions({"FLASK_DEBUG": value})
        assert debug is True

    @pytest.mark.parametrize("value", ["", "0", "false", "no", "off", "maybe"])
    def test_anything_else_leaves_debug_off(self, value):
        _, _, debug = run.serverOptions({"FLASK_DEBUG": value})
        assert debug is False

    def test_debug_on_loopback_is_allowed(self):
        host, _, debug = run.serverOptions(
            {"FLASK_DEBUG": "1", "HOST": "127.0.0.1"}
        )
        assert (host, debug) == ("127.0.0.1", True)

    def test_public_debug_is_refused(self):
        with pytest.raises(SystemExit, match="Refusing"):
            run.serverOptions({"FLASK_DEBUG": "1", "HOST": "0.0.0.0"})

    def test_public_debug_can_be_forced_deliberately(self, monkeypatch):
        monkeypatch.setenv("ALLOW_PUBLIC_DEBUG", "1")
        host, _, debug = run.serverOptions({"FLASK_DEBUG": "1", "HOST": "0.0.0.0"})
        assert (host, debug) == ("0.0.0.0", True)

    def test_a_public_bind_without_debug_is_fine(self):
        """Containers need this; it is the combination that is dangerous."""
        host, _, debug = run.serverOptions({"HOST": "0.0.0.0"})
        assert (host, debug) == ("0.0.0.0", False)

    def test_host_and_port_are_configurable(self):
        host, port, _ = run.serverOptions({"HOST": "10.0.0.5", "PORT": "8080"})
        assert (host, port) == ("10.0.0.5", 8080)


class TestSecretKey:
    def test_comes_from_the_environment(self, monkeypatch):
        monkeypatch.setenv("SECRET_KEY", "from-the-environment")
        reloaded = importlib.reload(config)
        assert reloaded.SECRET_KEY == "from-the-environment"

    def test_absent_key_falls_back_to_a_random_one(self, monkeypatch, capsys):
        monkeypatch.delenv("SECRET_KEY", raising=False)
        first = importlib.reload(config).SECRET_KEY
        warning = capsys.readouterr().out
        second = importlib.reload(config).SECRET_KEY
        assert first != second, "the fallback must not be a fixed value"
        assert len(first) >= 32
        assert "SECRET_KEY is not set" in warning, "a silent fallback is worse"

    def test_no_secret_is_committed(self):
        """The key that used to live here is in the repository history; make sure
        a new one does not get added back."""
        source = open("config.py").read()
        assert "bux0fohngaeva6ree2Fie7tie" not in source
        assert "os.environ" in source


class TestCsrfIsOffForAReason:
    """CSRF is disabled because nothing here changes state and the form is a GET.
    These tests fail if that stops being true, which is the point: the decision
    has to be revisited then, not silently inherited."""

    def test_csrf_is_disabled(self):
        assert config.WTF_CSRF_ENABLED is False

    def test_no_route_accepts_a_state_changing_method(self):
        from app import app as flask_app

        unsafe = {"POST", "PUT", "PATCH", "DELETE"}
        offenders = {
            rule.rule: sorted(rule.methods & unsafe)
            for rule in flask_app.url_map.iter_rules()
            if rule.methods & unsafe
        }
        assert not offenders, (
            f"{offenders} accept state-changing methods while CSRF is disabled - "
            f"turn WTF_CSRF_ENABLED on and move the form to POST"
        )


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
        assert "run.py" not in startup, "the Flask dev server must not serve traffic"

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
        """A cold Open-Meteo fetch plus rendering can exceed gunicorn's 30s
        default, which would kill the worker mid-request."""
        assert "--timeout" in startup

    def test_container_does_not_run_as_root(self):
        dockerfile = open("Dockerfile").read()
        assert "USER meteogram" in dockerfile

    def test_no_secret_is_baked_into_the_image(self):
        dockerfile = open("Dockerfile").read()
        assert "ENV SECRET_KEY" not in dockerfile
