from bdc_db.models import db, Collection
from marshmallow_sqlalchemy.schema import ModelSchema


class CollectionForm(ModelSchema):
    class Meta:
        model = Collection
        sqla_session = db.session