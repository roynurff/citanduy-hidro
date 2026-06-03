from flask_wtf import FlaskForm

from wtforms import StringField, TextAreaField, SelectField, HiddenField, FloatField, DateField
from wtforms.validators import DataRequired
from flask_wtf.file import FileField, FileAllowed, FileRequired



class UserForm(FlaskForm):
    username = StringField('username', validators=[DataRequired()])
    password = StringField('password', validators=[DataRequired()])
    pos = SelectField('pos', choices=[])
    
class PasswordForm(FlaskForm):
    username = HiddenField('username')
    password = StringField('password', validators=[DataRequired()])
    
class CurahHujanForm(FlaskForm):
    pos = HiddenField('pos_id')
    sampling = HiddenField('sampling')
    ch = FloatField('curahhujan')
    fetch = HiddenField('fetch')
    
class TmaForm(FlaskForm):
    pos = HiddenField('pos_id')
    sampling = HiddenField('sampling')
    jam = SelectField('jam', choices=[('07', '07'), ('12', '12'), ('17', '17')])
    tma = FloatField('tma')
    fetch = HiddenField('fetch')

class NoteForm(FlaskForm):
    obj_name = HiddenField('obj_name')
    obj_id = HiddenField('obj_id')
    msg = StringField('msg')
    
class KlimatForm(FlaskForm):
    pos = HiddenField('pos_id')
    sampling = HiddenField('sampling')
    jam = SelectField('jam', choices=[('07', '07'), ('12', '12'), ('17', '17')])
    thermometer_max = FloatField('tmax')
    thermometer_min = FloatField('tmin')
    bola_basah = FloatField('bb')
    bola_kering = FloatField('bk')
    thermometer_apung_max = FloatField('tamax')
    thermometer_apung_min = FloatField('tamin')
    penguapan = FloatField('p')
    anemometer = FloatField('a') # km/jam
    rh = FloatField('rh') # Relatif Humidity dalam %
    
class HasilUjiKAForm(FlaskForm):
    pos = HiddenField('pos_id')
    sampling = DateField('sampling', validators=[DataRequired()])
    lokasi = StringField('lokasi')
    sungai = StringField('sungai')
    kota_kabupaten = StringField('kota_kabupaten')
    ll = StringField('ll')
    pi = FloatField('pi', validators=[DataRequired()])
    kelas_baku_mutu = SelectField('kelas_baku_mutu', choices=[(1, 'Kelas 1'), (2, 'Kelas 2'), (3, 'Kelas 3'), (4, 'Kelas 4')], default='2', validators=[DataRequired()])
    keterangan = StringField('keterangan')
    periode = SelectField('periode', choices=[(1, 'Periode I'), (2, 'Periode II'), (3, 'Periode III')], validators=[DataRequired()])
    lembaga = StringField('lembaga')
    fname = StringField('fname')
    foto = FileField('foto', validators=[FileAllowed(['jpg', 'jpeg', 'png', 'pdf'], 'Images and PDF only!')])
    

class PublikasiForm(FlaskForm):
    title = StringField('title', validators=[DataRequired()])
    sampling = DateField('sampling', validators=[DataRequired()])
    content = StringField('content', validators=[DataRequired()])
    saran = TextAreaField('saran')
    tags = StringField('tags', validators=[DataRequired()])
    filename = FileField('filename', validators=[FileRequired(), FileAllowed(['pdf'], 'PDF only!')])

class EditPublikasiForm(FlaskForm):                         
    """Form edit — filename opsional (boleh kosong kalau tidak ganti PDF)"""
    title    = StringField('title', validators=[DataRequired()])
    sampling = DateField('sampling', validators=[DataRequired()])
    content  = StringField('content', validators=[DataRequired()])
    saran    = TextAreaField('saran')
    tags     = StringField('tags', validators=[DataRequired()])
    filename = FileField('filename', validators=[
                   FileAllowed(['pdf'], 'PDF only!')])
    
class TicketForm(FlaskForm):
    username = HiddenField('username', validators=[DataRequired()])
    subject = StringField('subject', validators=[DataRequired()])
    message = StringField('message', validators=[DataRequired()])
    pic = StringField('pic', validators=[DataRequired()])
    status = HiddenField('status', default='0')
    tags = StringField('tags')
