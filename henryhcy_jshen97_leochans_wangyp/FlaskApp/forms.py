from flask_wtf import FlaskForm
from wtforms import RadioField, SelectField, SubmitField, StringField, TextAreaField
from wtforms.validators import DataRequired


class RateForm(FlaskForm):
    Name = StringField('Please enter your name or nickname:', [DataRequired()])
    Ratings = RadioField('Please rate our project out of 5:', choices=[('5', '5'), ('4', '4'), ('3', '3'), ('2', '2'), ('1', '1')])
    Comments = TextAreaField('Please leave your comments or advices:', render_kw={"rows": 7, "cols": 50})
    Submit = SubmitField('Submit')


class Task2From(FlaskForm):
    Stores = SelectField('Store1--Please select the first CVS Pharmacy store:', choices=[
        ('None', 'None'),
        ('ChIJdyNdJqNw44kRZ6xCJS0GLpE', '100 Cambridgeside Pl Spc E122, Cambridge'),
        ('ChIJQWZTEX9w44kR2x78FGSg6ig', '85 Seaport Blvd, Boston'),
        ('ChIJ2_f1-IFw44kRJ7rT0PnUaEY', '700 Atlantic Ave, Boston'),
        ('ChIJIbfjHVtw44kRilwiZbjqHwY', '210 Border St, East Boston'),
        ('ChIJA0kd-INw44kRD2Gkwg8S53w', '81 Milk St, Boston'),
        ('ChIJXZ6_45hw44kR3rwobBwTWMk', '155 Charles St, Boston'),
        ('ChIJ5XvgznN644kRMJpz9omJxP8', '400 Tremont St, Boston'),
        ('ChIJo7uxpI1544kRqbqcQp8DDeI', '350 Longwood Ave, Boston'),
        ('ChIJvzt-LYx544kRcz7UXq2GCMk', '300 Longwood Ave, Boston'),
        ('ChIJBb4NfAx644kRolNloBH20qA', '587 Boylston St, Boston'),
        ('ChIJvWp9j4l644kRLnPmI1QWDZY', '423 W Broadway, South Boston'),
        ('ChIJF87GzYhw44kR-BsX2KEpYiU', '101 Canal St suite a, Boston'),
        ('ChIJF87GzYhw44kRvh_eY_YjScI', '218 Hanover St, Boston'),
        ('ChIJP6PMwx1644kRmKYzl_ffe3E', '1249 Boylston St, Boston'),
        ('ChIJk0WXQZpw44kRrVI6v8Plu1I', '191 Cambridge St, Boston'),
        ('ChIJFSqZ5Xd644kRHLIfujyv1Ow', '631 Washington St, Boston'),
        ('ChIJNdmJ34Rw44kRD_djUN2wMEE', '2 Center Plaza, Boston')
    ])

    Compare = SelectField('Store2--Please select the second store you want to compare with store1:', choices=[
        ('None', 'None'),
        ('ChIJdyNdJqNw44kRZ6xCJS0GLpE', '100 Cambridgeside Pl Spc E122, Cambridge'),
        ('ChIJQWZTEX9w44kR2x78FGSg6ig', '85 Seaport Blvd, Boston'),
        ('ChIJ2_f1-IFw44kRJ7rT0PnUaEY', '700 Atlantic Ave, Boston'),
        ('ChIJIbfjHVtw44kRilwiZbjqHwY', '210 Border St, East Boston'),
        ('ChIJA0kd-INw44kRD2Gkwg8S53w', '81 Milk St, Boston'),
        ('ChIJXZ6_45hw44kR3rwobBwTWMk', '155 Charles St, Boston'),
        ('ChIJ5XvgznN644kRMJpz9omJxP8', '400 Tremont St, Boston'),
        ('ChIJo7uxpI1544kRqbqcQp8DDeI', '350 Longwood Ave, Boston'),
        ('ChIJvzt-LYx544kRcz7UXq2GCMk', '300 Longwood Ave, Boston'),
        ('ChIJBb4NfAx644kRolNloBH20qA', '587 Boylston St, Boston'),
        ('ChIJvWp9j4l644kRLnPmI1QWDZY', '423 W Broadway, South Boston'),
        ('ChIJF87GzYhw44kR-BsX2KEpYiU', '101 Canal St suite a, Boston'),
        ('ChIJF87GzYhw44kRvh_eY_YjScI', '218 Hanover St, Boston'),
        ('ChIJP6PMwx1644kRmKYzl_ffe3E', '1249 Boylston St, Boston'),
        ('ChIJk0WXQZpw44kRrVI6v8Plu1I', '191 Cambridge St, Boston'),
        ('ChIJFSqZ5Xd644kRHLIfujyv1Ow', '631 Washington St, Boston'),
        ('ChIJNdmJ34Rw44kRD_djUN2wMEE', '2 Center Plaza, Boston')
    ])

    Total = SelectField('Store3--Please select the third store to see the total stability & accessibility:', choices=[
        ('None', 'None'),
        ('ChIJdyNdJqNw44kRZ6xCJS0GLpE', '100 Cambridgeside Pl Spc E122, Cambridge'),
        ('ChIJQWZTEX9w44kR2x78FGSg6ig', '85 Seaport Blvd, Boston'),
        ('ChIJ2_f1-IFw44kRJ7rT0PnUaEY', '700 Atlantic Ave, Boston'),
        ('ChIJIbfjHVtw44kRilwiZbjqHwY', '210 Border St, East Boston'),
        ('ChIJA0kd-INw44kRD2Gkwg8S53w', '81 Milk St, Boston'),
        ('ChIJXZ6_45hw44kR3rwobBwTWMk', '155 Charles St, Boston'),
        ('ChIJ5XvgznN644kRMJpz9omJxP8', '400 Tremont St, Boston'),
        ('ChIJo7uxpI1544kRqbqcQp8DDeI', '350 Longwood Ave, Boston'),
        ('ChIJvzt-LYx544kRcz7UXq2GCMk', '300 Longwood Ave, Boston'),
        ('ChIJBb4NfAx644kRolNloBH20qA', '587 Boylston St, Boston'),
        ('ChIJvWp9j4l644kRLnPmI1QWDZY', '423 W Broadway, South Boston'),
        ('ChIJF87GzYhw44kR-BsX2KEpYiU', '101 Canal St suite a, Boston'),
        ('ChIJF87GzYhw44kRvh_eY_YjScI', '218 Hanover St, Boston'),
        ('ChIJP6PMwx1644kRmKYzl_ffe3E', '1249 Boylston St, Boston'),
        ('ChIJk0WXQZpw44kRrVI6v8Plu1I', '191 Cambridge St, Boston'),
        ('ChIJFSqZ5Xd644kRHLIfujyv1Ow', '631 Washington St, Boston'),
        ('ChIJNdmJ34Rw44kRD_djUN2wMEE', '2 Center Plaza, Boston')
    ])

    K = SelectField('K-salesmen:', choices=[
        ('2', '2'), ('3', '3'), ('4', '4'), ('5', '5'), ('6', '6'),
        ('7', '7'), ('8', '8'), ('9', '9'), ('10', '10')
    ])

    S = StringField('Stability Threshold (must be an int):')
    A = StringField('Accessibility Threshold (must be an int):')

    Submit = SubmitField('Find')
