from flask_wtf import FlaskForm
from wtforms import RadioField, SubmitField, StringField, TextAreaField
from wtforms.validators import DataRequired


class RateForm(FlaskForm):
    Name = StringField('Please enter your name or nickname:', [DataRequired()])
    Ratings = RadioField('Please rate our project out of 5:', choices=[('5', '5'), ('4', '4'), ('3', '3'), ('2', '2'), ('1', '1')])
    Comments = TextAreaField('Please leave your comments or advices:', render_kw={"rows": 25, "cols": 50})
    Submit = SubmitField('Submit')
