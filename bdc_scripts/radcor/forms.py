from marshmallow_sqlalchemy import ModelSchema
from bdc_scripts.models import db
from bdc_scripts.radcor.models import RadcorActivity


class RadcorActivityForm(ModelSchema):
    class Meta:
        model = RadcorActivity
        sqla_session = db.session