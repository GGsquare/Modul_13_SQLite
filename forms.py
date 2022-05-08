from flask_wtf import FlaskForm
from wtforms import StringField, TextAreaField, BooleanField
from wtforms.validators import DataRequired

class TodoForm(FlaskForm):
    number = StringField()
    description = TextAreaField()
    done = BooleanField()