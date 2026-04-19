import functools
from flask import Flask, render_template, flash, redirect
from flask import jsonify, abort, request, url_for, Response
from flask_login import LoginManager, current_user, login_user, logout_user, login_required
from flask_wtf import FlaskForm
from flask_wtf.csrf import CSRFProtect
from wtforms import StringField, PasswordField, BooleanField, SubmitField
from wtforms.validators import DataRequired
from urllib.parse import urlparse, urljoin
from playhouse.flask_utils import FlaskDB
from dotenv import load_dotenv
from peewee import fn
import dateparser
from bs4 import BeautifulSoup
from gtts import gTTS
import pandas as pd

import requests
import datetime
import json
from typing import Optional
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

load_dotenv()

db_wrapper = FlaskDB()
csrf = CSRFProtect()

limiter = Limiter(
    key_func=get_remote_address,
    default_limits=["288 per day", "12 per hour"],
    storage_uri="memory://",
)

from app.models import FetchLog, User, RDaily, UserQuery, ManualDaily, Pos, OPos, NUM_DAYS
from app.forms import CurahHujanForm, TmaForm
from app.utils import request_handler
def get_warning_wlevel(now: Optional[datetime.datetime] = None) -> list:
    '''Mengambil daftar pos dengan level air tinggi
    Syarat output:
    - harus terdefinisikan siaga sh, sk, sm
    - harus ada wlevel di raw telemetri pada hari ini (now)
    - harus ada latlon yang valid
    '''
    if not now:
        now = datetime.datetime.now()
    rd = RDaily.select().where(RDaily.sampling==now.date())
    warning_list = []
    for r in rd:
        if not r.pos or not r.pos.sh or not r.pos.ll:
            continue
        pos = r.pos
        raw = json.loads(r.raw)
        if 'wlevel' not in raw[0]:
            continue
        #if float(raw[-1]['wlevel']) > 100.0:  # Threshold for warning, e.g., 100 cm
        #    pass
        if r.source == 'SC':
            wlevels = [(r['sampling'], float(r['wlevel'] * 100)) for r in raw]
        else:
            wlevels = [(r['sampling'], float(r['wlevel'])) for r in raw]
        status = 'normal'
        if wlevels[-1][1] >= pos.sm:
            status = 'siaga merah'
        elif wlevels[-1][1] >= pos.sk:
            status = 'siaga kuning'
        elif wlevels[-1][1] >= pos.sh:
            status = 'siaga hijau'
        warning_list.append(
            {
                'pos': 
                    {'nama': pos.nama,
                     'id': pos.id,
                     'latlon': pos.ll,
                     'elevasi': pos.elevasi,
                     'source': r.source,
                     'sh': pos.sh,
                     'sk': pos.sk,
                     'sm': pos.sm
                     },
                'sampling': wlevels[-1][0], 
                'wlevel': wlevels,
                'status': status
            })
    return warning_list

def get_heavy_rainfall(now: datetime.datetime = datetime.datetime.now()) -> list:
    rd = RDaily.select().where(RDaily.sampling==now.date())
    rain_list = []
    for r in rd:
        if not r.pos or not r.pos.ll or not r.pos.tipe in ('1', '3'):
            continue
        raw = json.loads(r.raw)
        if 'rain' not in raw[0]:
            continue
        pos = r.pos
        minute_start = datetime.datetime.fromisoformat(raw[-1]['sampling'])
        durasi = datetime.timedelta()
        hujan = 0
        if r.source in ('SA', 'SB'):
            for ra in reversed(raw):
                if 'rain' not in ra:
                    continue
                sampling = datetime.datetime.fromisoformat(ra['sampling'])
                if sampling < now - datetime.timedelta(minutes=60):
                    continue
                if float(ra['rain']) > 0.0:
                    durasi += minute_start - sampling
                    minute_start = sampling
                hujan += float(ra['rain'])
        else:
            l = raw[-1]['rain']
            for ra in reversed(raw):
                sampling = datetime.datetime.fromisoformat(ra['sampling'])
                if sampling < now - datetime.timedelta(minutes=60):
                    continue
                rain_now = l - ra['rain']
                if rain_now > 0.0:
                    durasi += minute_start - sampling
                    minute_start = sampling
                hujan += rain_now
                l = ra['rain']
                #click.echo('{} {}'.format(sampling.strftime('%H:%M'), ra['rain']))
        if hujan > 5.0:
            rain_list.append(
                {
                    'pos': 
                        {'nama': pos.nama, 
                        'id': pos.id,
                        'latlon': pos.ll,
                        'source': r.source,
                        'elevasi': pos.elevasi,
                        },
                    'sampling': raw[0]['sampling'], 
                    'rain': hujan, 
                    'duration': durasi.total_seconds()
                })
    return rain_list

def get_delayed_device() -> list:
    '''Mengambil daftar perangkat yang tidak mengirimkan data dalam 24 jam terakhir'''
    now = datetime.datetime.now()
    device_list = []
    for r in OPos.select().where(OPos.aktif==True, OPos.latest_sampling <= (now - datetime.timedelta(hours=2))):
        device_list.append({
            'id': r.id,
            'logger_id': r.nama,
            'source': r.source,
            'latest_sampling': r.latest_sampling.strftime('%Y-%m-%d %H:%M:%S'),
            'tipe': r.tipe,
            'll': r.pos.ll if r.pos else None,
            'nama': r.pos.nama if r.pos else None,
        })
    
    return device_list

def get_sampling(s: str="") -> list:
    try:
        sampling = datetime.datetime.strptime(s, '%Y-%m-%d')
        _sampling = sampling - datetime.timedelta(days=1)
        sampling_ = sampling + datetime.timedelta(days=1)
    except:
        sampling = datetime.datetime.now()
        _sampling = sampling - datetime.timedelta(days=1)
        sampling_ = None
    if sampling.date() >= datetime.date.today():
        sampling_ = None
    
    return (_sampling, sampling, sampling_)

login_manager = LoginManager()
login_manager.login_view = 'login'
login_manager.login_message = 'Silakan login untuk mengakses...'

def is_safe_url(target):
    ref_url = urlparse(request.host_url)
    test_url = urlparse(urljoin(request.host_url, target))
    return test_url.scheme in ('http', 'https') and \
        ref_url.netloc == test_url.netloc
        
def get_redirect_target():
    for target in request.values.get('next'), request.referrer:
        if not target:
            continue
        if is_safe_url(target):
            return target
        
def redirect_back(endpoint, **values):
    target = request.form['next']
    if not target or not is_safe_url(target):
        target = url_for(endpoint, **values)
    return redirect(target)

def admin_required(func):
    @functools.wraps(func)
    def myinner(*args, **kwargs):
        if current_user.pos:
            abort(404)
        return func(*args, **kwargs)
    return myinner
        
class LoginForm(FlaskForm):
    username = StringField('Username', validators=[DataRequired()])
    password = PasswordField('Password', validators=[DataRequired()])
    remember_me = BooleanField('Remember Me')
    submit = SubmitField('Sign In')
    
def create_app():
    app = Flask(__name__)
    #app.config['DATABASE'] = 'postgresql://citauser:thispass@localhost:5432/citadb'
    app.config.from_pyfile('config.py')
    db_wrapper.init_app(app)
    csrf.init_app(app)
    limiter.init_app(app)
    
    from app.cli import register as register_cli
    
    register_cli(app)
        
    register_bluprint(app)
    
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        try:
            return User.get(user_id)
        except User.DoesNotExist:
            return None
    
    @app.route('/ews')
    def ews():
        '''Endpoint untuk EWS, hanya untuk testing'''
        if request.args.get('format', 'html') == 'json':
            poses = Pos.select().order_by(Pos.nama)
            data = []
            for p in poses:
                data.append({
                    'id': p.id,
                    'nama': p.nama,
                    'kabupaten': p.kabupaten,
                    'sungai': p.sungai,
                    'elevasi': p.elevasi,
                    'tipe': p.tipe,
                    'll': p.ll
                })
            return jsonify(data)
        if request.args.get('warning'):
            warning_list = get_warning_wlevel()
            return jsonify(warning_list)
        if request.args.get('wlevel'):
            warning_list = get_warning_wlevel()
            return jsonify(warning_list)
        if request.args.get('rain'):
            rain_list = get_heavy_rainfall()
            return jsonify(rain_list)
        if request.args.get('device'):
            device_list = get_delayed_device()
            return jsonify(device_list)
        return render_template('ews.html')
    
    
    @app.route('/ai', methods=['GET', 'POST'])
    def chat():
        if request.method == 'POST':
            data = request.json
            user_text = data.get('text', '')
            q = request_handler(user_text)
            uq = UserQuery.create(q=user_text, intent=q.get('intent'))
            try:
                soup = BeautifulSoup(q.get('result').get('msg'), 'html.parser')
                to_voice = soup.get_text()
                speech = gTTS(text=to_voice, lang='id', slow=False)
                speech.save('app/static/audio/output.mp3')
            except:
                pass
            return jsonify(q)
        return render_template('ai.html')

    @app.route('/c', methods=['GET', 'POST'])
    def ch():
        if request.method == 'POST':
            data = request.json
            user_text = data.get('text', '')
            response = request_handler(user_text)
            #uq = UserQuery.create(q=user_text, intent=response)
            return jsonify(response)
        return render_template('chat.html')
        
    from flask import send_from_directory
    import os
    @app.route('/google91b0d3511e72c1af.html')
    def google_verify():
        try:
            return send_from_directory(os.path.join(os.getcwd(), 'app/static'), 'google91b0d3511e72c1af.html')
        except:
            abort(404)

    @app.route('/sitemap.xml')
    def sitemap():
        try:
            return send_from_directory(os.path.join(os.getcwd(), 'app/static'), 'sitemap.xml')
        except:
            abort(404)
            
    @app.route('/download', methods=['GET', 'POST'])
    @login_required
    def download():
        if request.method == 'POST':
            if request.form.get('sumber') == 'telemetri':
                # Download data telemetri per tipe pos, untuk Laporan Siaga
                sampling = request.form.get('sampling')
                tipe = request.form.get('tipe')
                if tipe == '1':
                    fname = 'CurahHujan_' + sampling + '.csv'
                    pos_ids = [p.id for p in Pos.select().where(Pos.tipe.in_(['1', '3'])).order_by(Pos.nama)]
                else:
                    fname = 'TinggiMukaAir_' + sampling + '.csv'
                    pos_ids = [p.id for p in Pos.select().where(Pos.tipe==tipe)]
                rd = RDaily.select().where(RDaily.sampling==sampling, RDaily.pos_id.in_(pos_ids))
                csv_data = tipe == '1' and 'Curah Hujan ' or 'Tinggi Muka Air'
                csv_data += ' ' + sampling + '\n'
                csv_data += 'Diunduh: {}\n'.format(datetime.datetime.now().strftime('%d %b %Y jam %H:%M:%S'))
                jam = ','.join([str(i) for i in range(0, 24)]) + '\n'
                if tipe == '1':
                    try:
                        jam = ','.join([str(d) for d in list(rd[0]._rain().get('hourly').keys())])
                    except:
                        pass
                csv_data += 'Pos, Kabupaten,' + jam + '\n'

                for r in rd:
                    if tipe == '1':
                        data = ''
                        if r._rain() != None:
                            data = ['{:.1f}'.format(d.get('rain')) for d in list(r._rain().get('hourly').values())]
                    else:
                        data = [str(d.get('wlevel')) for d in r._24jam().values()]
                    csv_data += r.pos.nama + ',' + (r.pos.kabupaten or '-') + ',' + ','.join(data) + '\n'
                response = Response(csv_data, content_type="text/csv")
                response.headers["Content-Disposition"] = "attachment; filename={}".format(fname)
                return response
            elif request.form.get('periode') == 'sebulan':
                # download data Telemetri dan manual per pos atau semua pos
                pos_id = request.form.get('pos_id')
                sampling_str = request.form.get('sampling', datetime.date.today().strftime('%Y-%m'))
                print('pos_id', pos_id, 'sampling', sampling_str)
                # Parsing tanggal dan validasi input
                sampling_date = dateparser.parse(sampling_str)
                print('sampling_date', sampling_date)
#                if not sampling_date:
#                    return abort(404, "Invalid sampling date format.")

                is_pch = pos_id and pos_id.startswith('pch_')
                
                # Filter data berdasarkan bulan/tahun
                sampling_month_year = sampling_date.strftime('%Y-%m')
                
                pos_ids = []
                if pos_id == 'pch_all':
                    pos_ids = [p.id for p in Pos.select().where(Pos.tipe.in_(['1', '3'])).order_by(Pos.nama)]
                elif pos_id == 'pda_all':
                    pos_ids = [p.id for p in Pos.select().where(Pos.tipe=='2').order_by(Pos.nama)]
                elif pos_id and pos_id.startswith(('pch_', 'pda_')):
                    try:
                        print('pos_id', pos_id)
                        pos_ids = [int(pos_id.split('_')[1])]
                        # query data satu pos sebulan hasilkan data per jam, telemetri saja
                        pos = Pos.get(Pos.id==pos_ids[0])
                        fname = '{}_{}.csv'.format(pos.nama.replace(' ', '_'), sampling_date.strftime('%Y-%m'))
                        tipe = pos.tipe == '2' and 'Tinggi Muka Air' or 'Curah Hujan'
                        header = F'''{tipe} Jam-Jaman

Nama Pos:,{pos.nama}, Elevasi:,{pos.elevasi}
No Stasiun:,{pos.register}, Tipe Alat:,Telemetri
Bujur:,{pos.ll.split(',')[1]},Pemilik:,BBWS Citanduy
Lintang:,{pos.ll.split(',')[0]},Th. Pendirian:,-

Data {tipe} Bulan {sampling_date.strftime('%b %Y')} Telemetri

'''

                        # Fetch data from Peewee
                        rd_query = RDaily.select().where(
                            fn.TO_CHAR(RDaily.sampling, 'YYYY-MM') == sampling_month_year, 
                            RDaily.pos_id.in_(pos_ids)).order_by(RDaily.sampling)

                        man_query = ManualDaily.select().where(
                            fn.TO_CHAR(ManualDaily.sampling, 'YYYY-MM') == sampling_month_year, 
                            ManualDaily.pos_id.in_(pos_ids)).order_by(ManualDaily.sampling)

                        df_tele = pd.DataFrame()
                        
                        if is_pch:
                            telemetri = []
                            for t in rd_query:
                                rain_data = t._rain().get('hourly')
                                row = [t.sampling.strftime('%d'), t._rain().get('rain24')]
                                for hour, val in rain_data.items():
                                    row.append(round(val.get('rain'), 1))
                                telemetri.append(row)
                            
                            df_tele = pd.DataFrame(telemetri, columns=['tanggal', 'telemetri', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '0', '1', '2', '3', '4', '5', '6'])
                            
                            manual = [(m.sampling.strftime('%d'), m.ch) for m in man_query]
                            df_manual = pd.DataFrame(manual, columns=['tanggal', 'manual'])
                            df_merged = pd.merge(df_tele, df_manual, on='tanggal', how='outer')
                            df_merged = df_merged[['tanggal', 'telemetri', 'manual', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '0', '1', '2', '3', '4', '5', '6']]
                            extra_lines = F"Jumlah, {df_tele['telemetri'].sum()}, {df_manual['manual'].sum()}\n"
                            extra_lines += F"Rata-rata, {round(df_tele['telemetri'].mean(), 1)}, {round(df_manual['manual'].mean())}\n"
                            extra_lines += F"Maksimum, {df_tele['telemetri'].max()}, {df_manual['manual'].max()}\n"
                        else: # is_pda
                            telemetri = []
                            
                            for r in rd_query:
                                row = [r.sampling.strftime('%d')]
                                tma_now = 0
                                num_data = 0
                                for k, v in r._24jam().items():
                                    wlevel = v.get('wlevel', None)
                                    if wlevel:
                                        tma_now += wlevel
                                        num_data += 1 
                                    row.append(round(v.get('wlevel') * 0.01, 2))
                                rerata = round((tma_now / num_data if num_data > 0 else 0), 2)
                                row.append(round(rerata, 1))
                                telemetri.append(row)

                            df_tele = pd.DataFrame(telemetri, columns=['tanggal', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', 'harian'])
                            
                            maksimum = df_tele[['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23']].max()
                            minimum = df_tele[['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23']].min()
                            mean = df_tele[['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23']].mean()
                            maks = maksimum.to_frame().T
                            mins = minimum.to_frame().T
                            means = mean.to_frame().T
                            means = means.round(1)
                            mins.insert(0, 'tanggal', 'Minimum')
                            means.insert(0, 'tanggal', 'Rata-rata')
                            maks.insert(0, 'tanggal', 'Maksimum')
                            #maks.insert(len(maks.columns), 'harian', maksimum.max())
                            maks['harian'] = round(maksimum.mean(), 1)
                            mins['harian'] = minimum.min()
                            means['harian'] = round(mean.mean(), 1)
                            df_merged = pd.concat([df_tele, maks, means, mins], ignore_index=True)
                            #df_merged = pd.DataFrame()
                        
                        # Mengubah DataFrame menjadi CSV dan mengirimkannya
                        csv_output = df_merged.to_csv(index=False)
                        csv_output = header + csv_output  # Menambahkan newline di awal file jika diperlukan
                        
                        response = Response(csv_output, content_type="text/csv")
                        response.headers["Content-Disposition"] = f"attachment; filename={fname}"
                        return response
                        # ENDOF satu pos sebulan per jam

                    except (ValueError, IndexError) as e:
                        print(e)
                        raise ValueError("Invalid pos_id format.")
                        
                        #return abort(404, "Invalid pos_id format.")
                else:
                    return abort(404, "Invalid pos_id.")
                
                # Fetch data from Peewee
                rd_query = RDaily.select().where(
                    fn.TO_CHAR(RDaily.sampling, 'YYYY-MM') == sampling_month_year, 
                    RDaily.pos_id.in_(pos_ids)).order_by(RDaily.pos_id, RDaily.sampling)
                
                man_query = ManualDaily.select().where(
                    fn.TO_CHAR(ManualDaily.sampling, 'YYYY-MM') == sampling_month_year, 
                    ManualDaily.pos_id.in_(pos_ids)).order_by(ManualDaily.pos_id, ManualDaily.sampling)
                
                df_tele = pd.DataFrame()
                df_man = pd.DataFrame()
                
                # Konversi Peewee query ke DataFrame
                if is_pch:
                    fname = 'Hujan_{}.csv'.format(sampling_month_year)
                    telemetri = []
                    for r in rd_query:
                        try:
                            telemetri.append((r.pos.nama, r.pos.kabupaten, r.sampling, r._rain()['rain24']))
                        except TypeError:
                            pass
                    manual = [(m.pos.nama, m.pos.kabupaten, m.sampling, m.ch) for m in man_query]
                    
                    df_tele = pd.DataFrame(telemetri, columns=['nama', 'kabupaten', 'sampling', 'cht'])
                    df_man = pd.DataFrame(manual, columns=['nama', 'kabupaten', 'sampling', 'chm'])
                    
                else: # is_pda
                    fname = 'TMA_{}.csv'.format(sampling_month_year)
                    telemetri = [(r.pos.nama, r.pos.kabupaten, r.sampling, r._tma()[7].get('wlevel'), r._tma()[12].get('wlevel'), r._tma()[17].get('wlevel')) for r in rd_query]
                    manual = [(m.pos.nama, m.pos.kabupaten, m.sampling, m._tma.get('07'), m._tma.get('12'), m._tma.get('17')) for m in man_query]

                    df_tele = pd.DataFrame(telemetri, columns=['nama', 'kabupaten', 'sampling', 'T07', 'T12', 'T17'])
                    df_man = pd.DataFrame(manual, columns=['nama', 'kabupaten', 'sampling', 'M07', 'M12', 'M17'])
                    
                    kolom_target = ['T07', 'T12', 'T17']
                    df_tele[kolom_target] = df_tele[kolom_target].apply(pd.to_numeric, errors='coerce').astype('Int64')
                    
                df_tele['sampling'] = pd.to_datetime(df_tele['sampling'])
                df_man['sampling'] = pd.to_datetime(df_man['sampling'])

                # Gabungkan kedua DataFrame
                df_out = pd.merge(df_tele, df_man, on=['nama', 'sampling'], how='outer', suffixes=('_tele', '_man'))
                # Buat kolom 'key' baru dan tentukan kolom nilai
                if 'kabupaten_tele' in df_out.columns:
                    df_out['kabupaten'] = df_out['kabupaten_tele'].fillna(df_out['kabupaten_man'])
                else:
                    df_out['kabupaten'] = df_out['kabupaten_man'].fillna(df_out['kabupaten_tele'])

                df_out['pos'] = df_out['nama']
                df_out['kabupaten'] = df_out['kabupaten'].fillna('-')
                
                # Tentukan kolom yang akan di-pivot
                if is_pch:
                    file_header = 'Data Curah Hujan Bulan {} Telemetri & Manual\n'.format(sampling_date.strftime('%b %Y'))
                    columns_to_pivot = ['cht', 'chm']
                else:
                    file_header = 'Data Muka Air Bulan {} Telemetri & Manual\n'.format(sampling_date.strftime('%b %Y'))
                    columns_to_pivot = ['T07', 'T12', 'T17', 'M07', 'M12', 'M17']

                # Lakukan pivot table
                df_pivoted = df_out.pivot_table(
                    index=['pos', 'kabupaten'], columns='sampling', values=columns_to_pivot, aggfunc='first')
                
                # Handle kasus di mana tidak ada data sama sekali
                if df_pivoted.empty:
                    return jsonify({"message": "No data found for the selected period."}), 200

                # Reindex dengan rentang tanggal penuh untuk bulan tersebut
                start_date = sampling_date.replace(day=1)
                end_date = sampling_date.replace(day=1, month=sampling_date.month+1) - datetime.timedelta(days=1) if sampling_date.month != 12 else sampling_date.replace(day=31)
                all_dates_in_month = pd.date_range(start=start_date, end=end_date, freq='D')

                full_multi_index = pd.MultiIndex.from_product([columns_to_pivot, all_dates_in_month], names=['variable', 'date'])
                df_reindexed = df_pivoted.reindex(columns=full_multi_index)
                
                # 5. Get the correct, alternating order of column names
                #    We will iterate through each date and create two entries for it.
                ordered_cols = []
                for date in all_dates_in_month:
                    day_str = date.strftime('%d')
                    if is_pch:
                        ordered_cols.append(f"cht_{day_str}")
                        ordered_cols.append(f"chm_{day_str}")
                    else:
                        ordered_cols.append(f"T07_{day_str}")
                        ordered_cols.append(f"M07_{day_str}")
                        ordered_cols.append(f"T12_{day_str}")
                        ordered_cols.append(f"M12_{day_str}")
                        ordered_cols.append(f"T17_{day_str}")
                        ordered_cols.append(f"M17_{day_str}")

                # Meratakan multi-index kolom dan format nama
                df_reindexed.columns = [f"{col[0]}_{col[1].strftime('%d')}" for col in df_reindexed.columns]
                df_final = df_reindexed.reindex(columns=ordered_cols)
                
                df_final = df_final.reset_index()
                
                # Mengubah DataFrame menjadi CSV dan mengirimkannya
                csv_output = df_final.to_csv(index=False)
                csv_output = file_header + csv_output  # Menambahkan newline di awal file jika diperlukan
                response = Response(csv_output, content_type="text/csv")
                response.headers["Content-Disposition"] = f"attachment; filename={fname}"
                return response
            # Download data manual per pos
            else:
                try:
                    data = request.form
                    
                    pos = Pos.get(int(data.get('pos_id')))
                except:
                    return abort(404)
                csv_data = pos.nama + '\n'
                csv_data += 'Tanggal Download: ' + datetime.date.today().strftime('%d-%m-%Y') + '\n'
                if pos.tipe in ('1', '3'):
                    csv_data += 'Tanggal, Curah hujan\n'
                    mds = ManualDaily.select(ManualDaily.sampling, 
                                            ManualDaily.ch).where(
                                                ManualDaily.pos_id==pos.id).order_by(
                                                    ManualDaily.sampling)
                    for m in mds:
                        csv_data += '{}, {}\n'.format(m.sampling, m.ch)
                elif pos.tipe == '2':
                    csv_data += 'Tanggal, TMA7, TMA12, TMA17, TMARata-rata\n'
                    mds = ManualDaily.select(ManualDaily.sampling, 
                                            ManualDaily.tma).where(
                                                ManualDaily.pos_id==pos.id).order_by(
                                                    ManualDaily.sampling)
                    for m in mds:
                        tmas = json.loads(m.tma)
                        tma = {'07': None, '12': None, '17': None}
                        for j in ('07', '12', '17'):
                            try:
                                tma[j] = tmas[j]
                            except KeyError:
                                pass
                        filled_tma = [t for t in tma.values() if t]
                        tmarerata = sum(filled_tma)/ len(filled_tma)
                        csv_data += '{}, {}, {}, {}, {:.1f}\n'.format(m.sampling, 
                                                                tma['07'], 
                                                                tma['12'],
                                                                tma['17'],
                                                                tmarerata)

                response = Response(csv_data, content_type="text/csv")
                return response
 
        poses = Pos.select().order_by(Pos.nama)
        pdas = [p for p in poses if p.tipe == '2']
        pchs = [p for p in poses if p.tipe in ('1', '3')]
        for p in pchs:
            p.yearly = [(y.year, y.count) for y in p.manualdaily_set.select(ManualDaily.sampling.year.alias('year'), 
                                              fn.Count(ManualDaily.id).alias('count'))
                .group_by(ManualDaily.sampling.year)
                .order_by(ManualDaily.sampling.year)]
            p.count = p.manualdaily_set.count()
        for p in pdas:
            p.count = p.manualdaily_set.count()
        ctx = {
            'pdas': pdas,
            'pchs': pchs,
            'today': datetime.date.today()
        }
        return render_template('download/index.html', ctx=ctx)
    
    
    @app.route('/login', methods=['GET', 'POST'])
    @limiter.exempt
    def login():
        if current_user.is_authenticated:
            return redirect(url_for('homepage'))
        form = LoginForm()
        next = get_redirect_target()
        if form.validate_on_submit():
            try:
                user = User.get(User.username==form.username.data)
            except User.DoesNotExist:
                flash('username atau password keliru')
                return redirect(url_for('login'))
            if user is None or not user.check_password(form.password.data):
                flash('username atau password keliru')
                return redirect(url_for('login'))
            login_user(user)
            user.last_login = datetime.datetime.now()
            user.save()
            return redirect_back('homepage')
        return render_template('login.html', title='Sign In', form=form, next=next)
    
    @app.route('/logout')
    def logout():
        logout_user()
        return redirect('/')    

    @app.route('/me')
    def profile():
        user = User.get(User.username==current_user.username)
        ctx = {
            'me': user
        }
        return render_template('profile.html', ctx=ctx)
    
    @app.errorhandler(404)
    def page_not_found(e):
        return render_template('404.html'), 404
    
    @app.errorhandler(429)
    def too_many_requests(e):
        return jsonify({
            'error': 'Slow and Low: Data hanya update setiap 5 menit',
            'message': 'Data hanya update setiap 5 menit, silakan coba lagi nanti'
            }), 429
    
    @app.route('/')
    def homepage():
        '''background: #2193b0;  /* fallback for old browsers */
        background: -webkit-linear-gradient(to right, #6dd5ed, #2193b0);  /* Chrome 10-25, Safari 5.1-6 */
        background: linear-gradient(to right, #6dd5ed, #2193b0); /* W3C, IE 10+/ Edge, Firefox 16+, Chrome 26+, Opera 12+, Safari 7+ */
        '''
        if current_user.is_authenticated and current_user.pos is not None:
            formhujan = CurahHujanForm()
            formtma = TmaForm()
            pos = current_user.pos
                
            (_sampling, sampling, sampling_) = get_sampling(request.args.get('s', None))
            today = datetime.date.today()
            if current_user.pos.tipe == '1' and sampling.strftime('%Y%m') == today.strftime('%Y%m') and today.day == 1:
                sampling -= datetime.timedelta(days=1)
            sampling = sampling.replace(day=1)
            _sampling = (sampling - datetime.timedelta(days=1)).replace(day=1)
            list_data = {}
            if sampling.strftime('%Y%m') == today.strftime('%Y%m'):
                list_data = dict([(i+1, {'tgl': sampling + datetime.timedelta(days=i+1)}) for i in range(today.day)])
                sampling_ = None
            elif sampling.strftime('%Y%m') < today.strftime('%Y%m'):
                sampling_ = (sampling.replace(day=1) + datetime.timedelta(days=32)).replace(day=1)
                list_data = dict([(i+1, {'tgl': sampling + datetime.timedelta(days=i+1)}) for i in range((sampling_ - datetime.timedelta(days=1)).day)])
            if current_user.pos.tipe in ('1', '3'):
                list_data = dict([(k, v) for k, v in list_data.items() if v['tgl'].date() <= today])    
            
            formhujan.sampling.data = sampling
            formtma.sampling.data = sampling
            mDaily = ManualDaily.select().where(ManualDaily.sampling.year==sampling.year,
                                                ManualDaily.sampling.month==sampling.month,
                                                ManualDaily.pos==pos)
            if pos.tipe in ('1', '3'):
                toDelete = [m for m in mDaily if m.ch is None]
                for td in toDelete:
                    td.delete_instance()
            
            data_manual = dict([(md.sampling.day, {'ch': md.ch, 'tma': md._tma}) for md in mDaily])
            for i, d in list_data.items():
                if i in data_manual:
                    list_data[i].update(data_manual.get(i))
            num_day = (sampling_ and (sampling_ - datetime.timedelta(days=1)).day or today.day)
            ctx = {
                'pos': pos,
                'list_data': list_data,
                'sampling': sampling,
                '_sampling': _sampling,
                'sampling_': sampling_,
                'formhujan': formhujan,
                'formtma': formtma,
                'num_day': num_day,
                'num_ch': len(data_manual),
                'num_tma': sum([len(v['tma']) for v in data_manual.values() if v['tma']])
            }
            template = 'home_petugas.html'
            return render_template(template, ctx=ctx)
        else:
            today = datetime.date.today()
            hujans = [r for r in RDaily.select().where(
                RDaily.sampling==today.strftime('%Y-%m-%d'), RDaily.pos!=None)]
            hujans = [r for r in hujans if r.pos.tipe in ('1', '3')]
            cimuntur = Pos.select().where(Pos.sungai.contains('cimuntur')).order_by(Pos.elevasi.desc())
            citanduy = Pos.select().where(Pos.sungai.contains('citanduy')).order_by(Pos.elevasi.desc())
            ctx = {
                'hujans': hujans,
                'today': today,
                'cimuntur': cimuntur,
                'citanduy': citanduy,
            }
            return render_template('index.html', ctx=ctx, canonical_url=url_for('homepage', _external=True))
                
    return app


def register_bluprint(app):
    from app.pch import bp as bp_pch
    from app.pda import bp as bp_pda
    from app.pos import bp as bp_pos
    from app.fetchlog import bp as bp_fetchlog
    from app.rdaily import bp as bp_rdaily
    from app.rpos import bp as bp_rpos
    from app.user import bp as bp_user
    from app.map import bp as bp_map
    from app.petugas import bp as bp_petugas
    from app.pklimat import bp as bp_klimat
    from app.pka import bp as bp_ka
    from app.kinerja import bp as bp_kinerja
    from app.api import bp as bp_api
    from app.api.rainfall import bp as bp_rainfall
    from app.note import bp as bp_note
    from app.publikasi import bp as bp_publikasi
    from app.ticket import bp as bp_ticket
    
    app.register_blueprint(bp_pch)
    app.register_blueprint(bp_pda)
    app.register_blueprint(bp_pos)
    app.register_blueprint(bp_fetchlog)
    app.register_blueprint(bp_rdaily)
    app.register_blueprint(bp_rpos)
    app.register_blueprint(bp_user)
    app.register_blueprint(bp_map)
    app.register_blueprint(bp_petugas)
    app.register_blueprint(bp_klimat)
    app.register_blueprint(bp_ka)
    app.register_blueprint(bp_kinerja)
    app.register_blueprint(bp_api)
    app.register_blueprint(bp_rainfall)
    app.register_blueprint(bp_note)
    app.register_blueprint(bp_publikasi)
    app.register_blueprint(bp_ticket)
