import datetime
import json
import calendar
from flask import Blueprint, render_template, request, jsonify, abort
from flask_login import current_user, login_required

from app.models import Pos, RDaily, ManualKlim
from app import get_sampling

bp = Blueprint('pklimat', __name__, url_prefix='/pklimat')

FIELDS = ['temp_min', 'temp_max', 'kelembaban', 'kec_angin',
          'arah_angin', 'lama_penyinaran', 'penguapan']

def deg_to_compass(val):
    if val is None:
        return '-'
    if isinstance(val, str):
        return val
    dirs = ['N','NNE','NE','ENE','E','ESE','SE','SSE',
            'S','SSW','SW','WSW','W','WNW','NW','NNW']
    idx = round(float(val) / 22.5) % 16
    return dirs[idx]


@bp.route('/')
def index():
    (_sampling, sampling, sampling_) = get_sampling(request.args.get('s', None))
    pos_klimats = []
    for p in Pos.select().where(Pos.tipe=='3').order_by(Pos.nama):
        # Data telemetri
        klimat = p.rdaily_set.where(
            RDaily.sampling==sampling.strftime('%Y-%m-%d')
        ).first()
        if klimat:
            data = json.loads(klimat.raw)
            last = data[-1]
            last['wind_dir_compass'] = deg_to_compass(last.get('wind_dir'))
            p.klimat = last
        else:
            p.klimat = None

        # Data manual — per hari, dict by date string
        mk = ManualKlim.select().where(
            ManualKlim.pos == p,
            ManualKlim.sampling == sampling.strftime('%Y-%m-%d')
        ).first()
        p.manual = mk

        pos_klimats.append(p)

    ctx = {
        'pklimats': pos_klimats,
        '_sampling': _sampling,
        'sampling': sampling,
        'sampling_': sampling_,
    }
    return render_template('pklimat/index.html', ctx=ctx)


@bp.route('/input')
@login_required
def input_index():
    if not current_user.is_admin:
        abort(403)
    pos_klimats = Pos.select().where(Pos.tipe=='3').order_by(Pos.nama)
    
    # Ambil bulan aktif dari query param, default bulan ini
    s = request.args.get('s', datetime.date.today().strftime('%Y-%m'))
    try:
        sampling = datetime.date.fromisoformat(s + '-01')
    except ValueError:
        sampling = datetime.date.today().replace(day=1)
    
    _sampling = (sampling - datetime.timedelta(days=1)).replace(day=1)
    next_month = (sampling.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
    sampling_ = None if next_month > datetime.date.today().replace(day=1) else next_month

    # Kalau ada pos_id, tampilkan tabel input
    pos_id = request.args.get('pos', None)
    selected_pos = None
    table_data = {}

    if pos_id:
        try:
            selected_pos = Pos.get(Pos.id == int(pos_id), Pos.tipe == '3')
        except Pos.DoesNotExist:
            abort(404)

        # Buat dict semua hari dalam bulan → data manual
        days_in_month = calendar.monthrange(sampling.year, sampling.month)[1]
        all_days = {
            datetime.date(sampling.year, sampling.month, d): None
            for d in range(1, days_in_month + 1)
        }
        manuals = ManualKlim.select().where(
            ManualKlim.pos == selected_pos,
            ManualKlim.sampling >= sampling,
            ManualKlim.sampling < next_month
        )
        for m in manuals:
            all_days[m.sampling] = m
        table_data = all_days

    ctx = {
        'pos_klimats': pos_klimats,
        'selected_pos': selected_pos,
        'sampling': sampling,
        '_sampling': _sampling,
        'sampling_': sampling_,
        'table_data': table_data,
        'fields': FIELDS,
    }
    return render_template('pklimat/input.html', ctx=ctx)


@bp.route('/<int:pos_id>/manual', methods=['POST'])
@login_required
def manual_save(pos_id):
    if not current_user.is_admin:
        return jsonify({'ok': False, 'msg': 'Tidak diizinkan'}), 403

    data = request.get_json()
    try:
        pos = Pos.get(Pos.id == pos_id, Pos.tipe == '3')
        sampling = data['sampling']  # 'YYYY-MM-DD'
        field = data['field']
        value = data['value']

        if field not in FIELDS:
            return jsonify({'ok': False, 'msg': 'Field tidak valid'}), 400

        mk, created = ManualKlim.get_or_create(
            pos=pos,
            sampling=sampling,
            defaults={'username': current_user.username}
        )

        setattr(mk, field, float(value) if value not in (None, '') else None)
        mk.username = current_user.username
        mk.mdate = datetime.datetime.now()
        mk.save()

        return jsonify({'ok': True, 'created': created})

    except Pos.DoesNotExist:
        return jsonify({'ok': False, 'msg': 'Pos tidak ditemukan'}), 404
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)}), 500


@bp.route('/<int:id>')
def show(id):
    try:
        pos = Pos.get(Pos.id==id, Pos.tipe=='3')
    except Pos.DoesNotExist:
        abort(404)
    (_sampling, sampling, sampling_) = get_sampling(request.args.get('s', None))
    _sampling = sampling.replace(year=sampling.year - 1)
    sampling_ = sampling.replace(year=sampling.year + 1)
    if datetime.date.today().year == sampling.year:
        sampling_ = None
    ctx = {
        'pos': pos,
        'sampling': sampling,
        '_sampling': _sampling,
        'sampling_': sampling_,
    }
    return render_template('pklimat/show.html', ctx=ctx)