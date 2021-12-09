import datetime
import os
import socket

from flask import Flask, jsonify, request
from flask_restx import Api, Resource, fields
from flask_sqlalchemy import SQLAlchemy
from google.cloud import secretmanager
from google.cloud.sql.connector import connector
from sqlalchemy.dialects import postgresql
from sqlalchemy import func
import waitress

app = Flask(__name__)
api = Api(
    app,
    version="1.0",
    title="Interview API",
    description="A simple API for Interview purposes",
)
ns = api.namespace("shows", description="Netflix Shows")


def access_secret_version(secret_id:str, version_id:str="latest") -> str:
    """Fetch secrets from GCP Secret Manager."""

    _project_id = os.environ.get("GCP_PROJECT", "theta-messenger-334101")
    # Create the Secret Manager client.
    _client = secretmanager.SecretManagerServiceClient()
    # Build the resource name of the secret version.
    _name = f"projects/{_project_id}/secrets/{secret_id}/versions/{version_id}"
    # Access the secret version.
    _response = _client.access_secret_version(name=_name)
    # Return the decoded payload.
    return _response.payload.data.decode("UTF-8")


def open_connection():
    _db_user = access_secret_version("DB_USER")
    _db_pass = access_secret_version("DB_PASS")
    _db_name = access_secret_version("DB_NAME")
    _conn = connector.connect(
        "theta-messenger-334101:us-central1:brettmoan-torqata",
        "pg8000",
        user=_db_user,
        password=_db_pass,
        db=_db_name,
    )
    return _conn


app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {"creator": open_connection}
# set the dialect to postgres. without this, the open_connection() is treated as sqllite
app.config[
    "SQLALCHEMY_DATABASE_URI"
] = "postgresql+pg8000://dummy:dummy@localhost/dummy"

db = SQLAlchemy(app)

# quote things that are sql identifiers
db.engine.dialect.identifier_preparer.quote("type")
db.engine.dialect.identifier_preparer.quote("cast")

show_model = api.model(
    "shows",
    {
        "show_id": fields.String(
            required=True, description="the unique identifier of a show"
        ),
        "type": fields.String(
            required=True, description="Movie or TV Show"
        ),
        "title": fields.String(
            required=True, description="Name of the Show"
        ),
        "director": fields.List(
            fields.String(
                required=True, description="name(s) of the director(s)"
            )
        ),
        "cast": fields.List(
            fields.String(required=True, description="the people in the show")
        ),
        "country": fields.List(
            fields.String(
                required=True, description="countries where show is available"
            )
        ),
        "date_added": fields.String(
            required=True, description="When show was added to netflix catalog"
        ),
        "release_year": fields.Integer(
            required=True, description="When show was origionally released"
        ),
        "rating": fields.String(
            required=True, description="MPAA rating"
        ),
        "duration": fields.String(
            required=True, description="How long the show is"
        ),
        "listed_in": fields.List(
            fields.String(
                required=True, description="Genres the show is listed in"
            )
        ),
        "description": fields.String(
            required=True, description="Summary of the Show"
        ),
    },
)


class Show(db.Model):
    __tablename__ = "shows_v3"
    show_id = db.Column(db.Text(), primary_key=True)
    type = db.Column(db.Text(), nullable=False)
    title = db.Column(db.Text(), nullable=False)
    director = db.Column(postgresql.ARRAY(db.Text()), nullable=True)
    cast = db.Column(postgresql.ARRAY(db.Text()), nullable=True)
    country = db.Column(postgresql.ARRAY(db.Text()), nullable=True)
    date_added = db.Column(db.Date(), nullable=False)
    release_year = db.Column(db.Integer, nullable=False)
    rating = db.Column(db.Text(), nullable=False)
    duration = db.Column(db.Text(), nullable=False)
    listed_in = db.Column(postgresql.ARRAY(db.Text()), nullable=True)
    description = db.Column(db.Text(), nullable=False)

    def __init__(
        self,
        show_id,
        type,
        title,
        director,
        cast,
        country,
        date_added,
        release_year,
        rating,
        duration,
        listed_in,
        description,
    ):
        self.show_id = show_id
        self.type = type
        self.title = title
        self.director = director
        self.cast = cast
        self.country = country
        self.date_added = date_added
        self.release_year = release_year
        self.rating = rating
        self.duration = duration
        self.listed_in = listed_in
        self.description = description

    def format(self):
        return {
            "show_id": self.show_id,
            "type": self.type,
            "title": self.title,
            "director": self.director,
            "cast": self.cast,
            "country": self.country,
            "date_added": self.date_added,
            "release_year": self.release_year,
            "rating": self.rating,
            "duration": self.duration,
            "listed_in": self.listed_in,
            "description": self.description,
        }


@ns.route("/")
@ns.param("page", "The page for pagination (defaults to 1)")
@ns.param("sort_by", "The columm to sort by")
@ns.param("sort_direction", "sort asc or desc")
@ns.param("show_id", "the unique identifier of a show")
@ns.param("type", "Movie or TV Show")
@ns.param("title", "Name of the Show")
@ns.param("director", "name(s) of the director(s)")
@ns.param("rating", "The MPAA rating")
@ns.param("cast", "the people in the show")
@ns.param("country", "countries where show is available")
@ns.param("date_added", "When show was added to netflix catalog in format '%Y-%m-%%d'")
@ns.param("release_year", "When show was origionally released")
@ns.param("duration", "How long the show is")
@ns.param("listed_in", "Genres the show is listed in")
@ns.param("description", "Summary of the Show")
class ShowsList(Resource):
    """meaningful comment here."""

    @ns.doc("list_shows")
    @ns.marshal_list_with(show_model)
    def get(self):
        """List all Shows (filterable)"""
        ROWS_PER_PAGE = 10
        _show_id = request.args.get("show_id", None, type=str)
        _type = request.args.get("type", None, type=str)
        _title = request.args.get("title", None, type=str)
        _director = request.args.get("director", None, type=str)
        _cast = request.args.get("cast", None, type=str)
        _country = request.args.get("country", None, type=str)
        _date_added = request.args.get("date_added", None, type=str)
        _release_year = request.args.get("release_year", None, type=str)
        _rating = request.args.get("rating", None, type=str)
        _duration = request.args.get("duration", None, type=str)
        _listed_in = request.args.get("listed_in", None, type=str)
        _description = request.args.get("description", None, type=str)

        _page = request.args.get("page", 1, type=int)
        _sort_by = request.args.get("sort_by", "show_id", type=str)
        _sort_direction = request.args.get("sort_direction", "asc", type=str)
        _sort_direction = (
            _sort_direction if _sort_direction in ("asc", "desc") else "asc"
        )

        # test URL
        # http://127.0.0.1:5000/shows/?cast=ryan%20reynolds&rating=PG-13
        #
        # Note
        # TEXT/INT/DATE -> Show.<>.ilike(<>)
        # ARRAY[TEXT] -> Show.<>.contains(<>)
        #
        _query = Show.query
        # append one or more filters
        if _show_id:
            _query = _query.filter(Show.show_id.ilike(_show_id))
        if _type:
            _query = _query.filter(Show.type.ilike(_type))
        if _title:
            _query = _query.filter(Show.title.ilike(_title))
        if _director:
            _query = _query.filter(Show.director.contains(_director))
        if _rating:
            _query = _query.filter(Show.rating.ilike(_rating))
        if _cast:
            _query = _query.filter(Show.cast.contains(_cast))
        if _country:
            _query = _query.filter(Show.country.contains(_country))
        if _date_added:
            _query = _query.filter(Show.date_added.ilike(_date_added))
        if _release_year:
            _query = _query.filter(Show.release_year.ilike(_release_year))
        if _duration:
            _query = _query.filter(Show.duration.ilike(_duration))
        if _listed_in:
            _query = _query.filter(Show.listed_in.contains(_listed_in))
        if _description:
            _query = _query.filter(Show.description.ilike(_description))

        # check to see if user passed sort flags
        if isinstance(_sort_by, str) and isinstance(_sort_direction, str):
            _sort_by = _sort_by.lower().replace(" ", "_")
            if _sort_by in show_model.keys():
                _sort_by_column = getattr(Show, _sort_by)
                _order_column = getattr(_sort_by_column, _sort_direction)
                _query = _query.order_by(_order_column())

        # paginate the response
        _paginated = _query.paginate(page=_page, per_page=ROWS_PER_PAGE).items
        _results = [s.format() for s in tuple(_paginated)]
        return _results

    @ns.doc("create_show")
    @ns.expect(show_model)
    @ns.marshal_with(show_model, code=201)
    def post(self):
        """Create/Update entry for a netflix show"""
        content = request.json
        # _show_id = request.args.get("show_id", None, type=str)
        if "show_id" not in content:
            # a little fun since its not prod code, 400 would be the correct response code.
            return ("The show id is mandatory. Also... I am a teapot.", 418)

        _existing_show = Show.query.filter(
            Show.show_id == str(content["show_id"])
        ).one_or_none()

        # Convert input string to python date object
        if content.get("date_added"):
            content["date_added"] = datetime.datetime.strptime(
                content["date_added"], "%Y-%m-%d"
            ).date()

        if _existing_show:
            # update all attributes from payload that correspond to a column
            for key, value in content.items():
                if key in show_model.keys():
                    setattr(_existing_show, key, value)

            # commit changes (if any)
            db.session.commit()
            return _existing_show.format()

        # There was no existing show, so instead lets create one.``
        new_show = Show(**content)
        # save the show to the database
        db.session.add(new_show)
        db.session.commit()
        return new_show.format()


@ns.route("/summary")
@ns.param("group_by", "The columns to group by (defaults to ['type', 'rating'])")
@ns.param("filter_column", "column to filter by (optional)")
@ns.param("filter_value", "Value to filter to (optional)")
class ShowsSummaryList(Resource):
    """This API has so far, never been a Teapot."""

    @ns.doc("summarize_shows")
    def get(self):
        """List summary of shows"""

        _group_by = request.args.get("group_by", [], type=list)
        _filter_value = request.args.get("filter_value", None, type=str)
        _filter_column = request.args.get("filter_column", None, type=str)

        # intersection of sets using & to only limit to valid keys
        _group_by_columns = list(set(show_model.keys()) & set(_group_by))
        # set default keys if list ended up empty
        _group_by_columns = (
            _group_by_columns if _group_by_columns else ["type", "rating"]
        )
        # convert strings to the actual attributes of `Show`
        _group_by_columns = [getattr(Show, column) for column in _group_by_columns]

        # list (positional) expansion of _group_by_columns using single *
        _query = Show.query.with_entities(
            *_group_by_columns, func.count(Show.show_id)
        ).group_by(*_group_by_columns)

        # if necessary apply a filter
        if isinstance(_filter_value, str) and isinstance(_filter_column, str):
            _filter_column = _filter_column.lower().replace(" ", "_")
            if _filter_column in ("cast", "director", "listed_in", "country"):
                # the array columns need to be passed as an array
                _filter_value = [_filter_value]
            if _filter_column in show_model.keys():
                filter_by_column = getattr(Show, _filter_column)
                _query = _query.filter(filter_by_column.contains(_filter_value))

        # the "Row" object isn't serializable to json. Cast as a dict to get
        # all the group by columns, then use row[-1] to get the count as I
        # listed the count last in the call to Show.query.with_entities()
        _results = [{**dict(row), "count": row[-1]} for row in _query.all()]

        return jsonify(
            {"success": True, "results": _results, "groups": len(_results)}
        )


if __name__ == "__main__":
    app.run(debug=False)
    waitress.serve(app, listen="0.0.0.0:5003")
