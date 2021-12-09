import socket
import os
import waitress
from flask import Flask, jsonify, request
from flask_restx import Api, Resource, fields
from flask_sqlalchemy import SQLAlchemy
from google.cloud.sql.connector import connector

app = Flask(__name__)
api = Api(
    app,
    version="1.0",
    title="Interview API",
    description="A simple API for Interview purposes",
)
ns = api.namespace("shows", description="Netflix Shows")

# app.config["SQLALCHEMY_ECHO"] = True

if socket.gethostname() == "DESKTOP-C32MC01":
    db_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "netflix.db"
    )
    app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{db_file_path}"
else:

    def access_secret_version(secret_id, version_id="latest"):
        from google.cloud import secretmanager

        PROJECT_ID = os.environ.get("GCP_PROJECT", "theta-messenger-334101")
        # Create the Secret Manager client.
        client = secretmanager.SecretManagerServiceClient()

        # Build the resource name of the secret version.
        name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version_id}"

        # Access the secret version.
        response = client.access_secret_version(name=name)

        # Return the decoded payload.
        return response.payload.data.decode("UTF-8")

    def open_connection():
        db_user = access_secret_version("DB_USER")
        db_pass = access_secret_version("DB_PASS")
        db_name = access_secret_version("DB_NAME")
        conn = connector.connect(
            "theta-messenger-334101:us-central1:brettmoan-torqata",
            "pg8000",
            user=db_user,
            password=db_pass,
            db=db_name,
        )
        return conn

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
            required=True, description="TODO description of show_id column"
        ),
        "type": fields.String(
            required=True, description="TODO description of type column"
        ),
        "title": fields.String(
            required=True, description="TODO description of title column"
        ),
        "director": fields.String(
            required=True, description="TODO description of director column"
        ),
        "cast": fields.String(
            required=True, description="TODO description of cast column"
        ),
        "country": fields.String(
            required=True, description="TODO description of country column"
        ),
        "date_added": fields.String(
            required=True, description="TODO description of date_added column"
        ),
        "release_year": fields.Integer(
            required=True, description="TODO description of release_year column"
        ),
        "rating": fields.String(
            required=True, description="TODO description of rating column"
        ),
        "duration": fields.String(
            required=True, description="TODO description of duration column"
        ),
        "listed_in": fields.String(
            required=True, description="TODO description of listed_in column"
        ),
        "description": fields.String(
            required=True, description="TODO description of description column"
        ),
    },
)


class Show_V3(db.Model):
    __tablename__ = "shows"
    show_id = db.Column(db.String(), primary_key=True)
    type = db.Column(db.String(), nullable=False)
    title = db.Column(db.String(), nullable=False)
    director = db.Column(db.String(), nullable=True)
    cast = db.Column(db.String(), nullable=True)
    country = db.Column(db.String(), nullable=False)
    date_added = db.Column(db.String(), nullable=False)
    release_year = db.Column(db.Integer, nullable=False)
    rating = db.Column(db.String(), nullable=False)
    duration = db.Column(db.String(), nullable=False)
    listed_in = db.Column(db.String(), nullable=False)
    description = db.Column(db.String(), nullable=False)

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


class Show(db.Model):
    __tablename__ = "shows"
    show_id = db.Column(db.String(), primary_key=True)
    type = db.Column(db.String(), nullable=False)
    title = db.Column(db.String(), nullable=False)
    director = db.Column(db.String(), nullable=True)
    cast = db.Column(db.String(), nullable=True)
    country = db.Column(db.String(), nullable=False)
    date_added = db.Column(db.String(), nullable=False)
    release_year = db.Column(db.Integer, nullable=False)
    rating = db.Column(db.String(), nullable=False)
    duration = db.Column(db.String(), nullable=False)
    listed_in = db.Column(db.String(), nullable=False)
    description = db.Column(db.String(), nullable=False)

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
@ns.param("page", "The page")
@ns.param("sort_by", "The sort_by")
@ns.param("sort_direction", "The sort_direction")
@ns.param("show_id", "The show_id")
@ns.param("type", "The type")
@ns.param("title", "The title")
@ns.param("director", "The director")
@ns.param("rating", "The rating")
@ns.param("cast", "The cast")
@ns.param("country", "The country")
@ns.param("date_added", "The date_added")
@ns.param("release_year", "The release_year")
@ns.param("duration", "The duration")
@ns.param("listed_in", "The listed_in")
@ns.param("description", "The description")
class ShowsList(Resource):
    """meaningful comment here."""

    @ns.doc("list_shows")
    @ns.marshal_list_with(show_model)
    def get(self):
        """List all Shows"""
        ROWS_PER_PAGE = 10
        _query = Show.query

        # TY to the post
        # https://betterprogramming.pub/how-to-implement-filtering-sorting-and-pagination-in-flask-c4def1ca004a
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
        # no commas -> Show.<>.ilike(<>)
        # has commas -> Show.<>.contains(<>)
        #
        # in a real world exampled we would do some data manipulation
        # and make these a list and make the appropriate changes to the object model
        if _type:
            _query = _query.filter(Show.type.ilike(_type))
        if _title:
            _query = _query.filter(Show.title.ilike(_title))
        if _director:
            _query = _query.filter(Show.director.ilike(_director))
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

        if isinstance(_sort_by, str) and isinstance(_sort_direction, str):
            _sort_by = _sort_by.lower().replace(" ", "_")
            if _sort_by in show_model.keys():
                sort_by_column = getattr(Show, _sort_by)
                # print(sort_by_column)
                order_column = getattr(sort_by_column, _sort_direction)
                # print(order_column)
                _query = _query.order_by(order_column())

        _shows = _query.paginate(page=_page, per_page=ROWS_PER_PAGE)

        _paginated = tuple(_shows.items)

        _results = [s.format() for s in _paginated]
        # http://127.0.0.1:5003/shows/?sort_by=show_id&sort_direction=desc

        # return jsonify({"success": True, "results": _results, "count": len(_results)}) # removed because after changing to `marshal_list_with()``
        return _results

    @ns.doc("create_todo")
    @ns.expect(show_model)
    @ns.marshal_with(show_model, code=201)
    def post(self):
        """Create a new Netflix show TODO"""
        content = request.json
        # _show_id = request.args.get("show_id", None, type=str)
        if "show_id" not in content:
            # a little fun since its not prod code, 400 would be the correct response code.
            return ("The show id is mandatory. Also... I am a teapot.", 418)

        # else: # above short circuits so pep8/pylint says not to use an else
        _existing_show = Show.query.filter(
            Show.show_id == str(content["show_id"])
        ).one_or_none()
        # _existing_show = Show.query.filter(Show.show_id == "s1").one()
        # Show.query.filter(Show.show_id == "s1").update({'rating': 'PG-14'})

        # _type = request.args.get("type", None, type=str)
        # _title = request.args.get("title", None, type=str)
        # _director = request.args.get("director", None, type=str)
        # _cast = request.args.get("cast", None, type=str)
        # _country = request.args.get("country", None, type=str)
        # _date_added = request.args.get("date_added", None, type=str)
        # _release_year = request.args.get("release_year", None, type=str)
        # _rating = request.args.get("rating", None, type=str)
        # _duration = request.args.get("duration", None, type=str)
        # _listed_in = request.args.get("listed_in", None, type=str)
        # _description = request.args.get("description", None, type=str)

        if _existing_show:
            # These Three lines
            for key, value in content.items():
                if key in show_model.keys():
                    setattr(_existing_show, key, value)

            # commit changes (if any)
            db.session.commit()
            return _existing_show.format()

        # else: # again, above short circuits so pep8/pylint says not to use an else
        new_show = Show(**content)
        db.session.commit()
        return new_show.format()


if __name__ == "__main__":
    pass
    # app.run(debug=True)
    waitress.serve(app, listen="0.0.0.0:5003")
