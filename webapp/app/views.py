from flask import render_template, request
from flask_wtf import FlaskForm
from wtforms import StringField, validators, SubmitField, DecimalField, IntegerField, RadioField
from app import app
from .controller import plotMeteogramFile
from meteogram.downloadJsonData import LocationNotFound
from meteogram.plotMeteogram import PLOT_TYPES, HresDataUnavailable
from base64 import b64encode
import os

TITLE = 'VSUP - Meteogram'

#Labels for the radio buttons. The values are PLOT_TYPES, which plotMeteogram
#validates against as well; test_webapp keeps the two in step.
PLOT_TYPE_LABELS = {
    "ensemble": "Pure Ensemble Data",
    "enhanced-hres": "HRES Enhanced Ensemble Data",
}

#The API gives 14 forecast days. Asking for more is clamped rather than
#rejected: the intent is unambiguous, we just run out of data.
MIN_DAYS = 1
MAX_DAYS = 14


class searchForm(FlaskForm):
    search = StringField("Search",  [validators.Optional()])
    lat = DecimalField("Latitude", [validators.Optional(),
                                    validators.NumberRange(min=-90, max=90)])
    lon = DecimalField("Longitude", [validators.Optional(),
                                     validators.NumberRange(min=-180, max=180)])
    days = IntegerField("Length of Meteogram in Days", default=3,
                        validators=[validators.Optional()])
    plotType = RadioField("Plottype",
                          choices=[(value, PLOT_TYPE_LABELS[value]) for value in PLOT_TYPES],
                          default='ensemble', validators=[validators.DataRequired()])
    submit = SubmitField('Go!')


def badRequest(form, error=None, status=400):
    #quick_form renders per-field errors itself, but not for a RadioField, so
    #anything the user would otherwise not see is passed through as `error`.
    return render_template("index.html", title=TITLE, form=form, error=error), status


@app.route('/', methods=("GET",))
def index():
    return render_template("index.html", title=TITLE, form=searchForm(request.args))


@app.route('/search', methods=("GET",))
def search():
    #Bind the form to the query string so the field validators above actually
    #run. validate_on_submit() is always False here - the form is submitted with
    #method="get" - which is why this used to read request.args by hand and
    #passed an unchecked plotType straight through to the renderer.
    form = searchForm(request.args)
    if not form.validate():
        return badRequest(form, error="; ".join(form.plotType.errors) or None)

    searchLocation = form.search.data or ""
    latitude = form.lat.data
    longitude = form.lon.data
    if not searchLocation and (latitude is None or longitude is None):
        return badRequest(
            form, error="Enter a place name, or both a latitude and a longitude.")

    days = form.days.data if form.days.data is not None else MIN_DAYS
    days = max(MIN_DAYS, min(MAX_DAYS, days))
    form.days.data = days

    try:
        filename = plotMeteogramFile(latitude=latitude, longitude=longitude,
                                     location=searchLocation,
                                     days=days,
                                     plotType=form.plotType.data)
    except LocationNotFound as exc:
        #The user typed a place we cannot resolve - their input, so a 400.
        return badRequest(form, error=str(exc))
    except HresDataUnavailable as exc:
        #Valid request, but this data source cannot serve it - 503, not 400.
        return badRequest(form, error=str(exc), status=503)
    path = os.path.join("/tmp", filename)
    try:
        with open(path, "rb") as fp:
            fileContent = b64encode(fp.read())
    finally:
        #Remove it even if the read failed, so a broken render does not leave
        #megabyte PNGs behind in /tmp.
        if os.path.exists(path):
            os.remove(path)
    return render_template("meteogram.html",
                           form=form,
                           plotType=form.plotType.data,
                           image='data:image/png;base64,{}'.format(fileContent.decode())
                           )
