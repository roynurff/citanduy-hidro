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

    import calendar

    # Navigasi — ambil dari query param
    today = datetime.date.today()
    tahun = int(request.args.get('y', today.year))
    bulan = int(request.args.get('m', today.month))
    mode  = request.args.get('mode', 'sehari')  # sehari, sebulan, setahun

    sampling = datetime.date(tahun, bulan, 1)
    days_in_month = calendar.monthrange(tahun, bulan)[1]
    next_month = (sampling.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)

    # Tahun tersedia (untuk dropdown)
    tahun_list = list(range(2023, today.year + 1))
    bulan_list = list(range(1, 13))

    # ── Data harian (sebulan) ──
    rdailies_bulan = {
        r.sampling: r for r in RDaily.select().where(
            RDaily.pos == pos,
            RDaily.sampling >= sampling,
            RDaily.sampling < next_month
        )
    }
    manuals_bulan = {
        m.sampling: m for m in ManualKlim.select().where(
            ManualKlim.pos == pos,
            ManualKlim.sampling >= sampling,
            ManualKlim.sampling < next_month
        )
    }

    # ── Data tahunan (agregasi per bulan) ──
    tahun_start = datetime.date(tahun, 1, 1)
    tahun_end   = datetime.date(tahun, 12, 31)
    rdailies_tahun  = list(RDaily.select().where(
        RDaily.pos == pos,
        RDaily.sampling >= tahun_start,
        RDaily.sampling <= tahun_end
    ))
    manuals_tahun = list(ManualKlim.select().where(
        ManualKlim.pos == pos,
        ManualKlim.sampling >= tahun_start,
        ManualKlim.sampling <= tahun_end
    ))

    # Agregasi telemetri per bulan (setahun)
    tele_per_bulan = {}
    for r in rdailies_tahun:
        bln = r.sampling.month
        if bln not in tele_per_bulan:
            tele_per_bulan[bln] = {'suhu': [], 'humidity': [], 'wind_speed': []}
        try:
            jam_data = r._24jam_klimat()
            temps  = [v['temperature'] for v in jam_data.values() if v['temperature'] is not None]
            humids = [v['humidity']    for v in jam_data.values() if v['humidity']    is not None]
            winds  = [v['wind_speed']  for v in jam_data.values() if v['wind_speed']  is not None]
            if temps:  tele_per_bulan[bln]['suhu'].append(sum(temps)/len(temps))
            if humids: tele_per_bulan[bln]['humidity'].append(sum(humids)/len(humids))
            if winds:  tele_per_bulan[bln]['wind_speed'].append(sum(winds)/len(winds))
        except Exception:
            pass

    tele_tahunan = {}
    for bln in range(1, 13):
        d = tele_per_bulan.get(bln, {})
        tele_tahunan[bln] = {
            'suhu':       round(sum(d.get('suhu', []))/len(d['suhu']), 1)       if d.get('suhu')       else None,
            'humidity':   round(sum(d.get('humidity', []))/len(d['humidity']), 1) if d.get('humidity')   else None,
            'wind_speed': round(sum(d.get('wind_speed', []))/len(d['wind_speed']), 1) if d.get('wind_speed') else None,
        }

    # Agregasi manual per bulan (setahun)
    manual_per_bulan = {}
    for m in manuals_tahun:
        bln = m.sampling.month
        if bln not in manual_per_bulan:
            manual_per_bulan[bln] = {
                'temp_min': [], 'temp_max': [], 'kelembaban': [],
                'kec_angin': [], 'lama_penyinaran': [], 'penguapan': []
            }
        for f in ('temp_min', 'temp_max', 'kelembaban', 'kec_angin', 'lama_penyinaran', 'penguapan'):
            val = getattr(m, f)
            if val is not None:
                manual_per_bulan[bln][f].append(val)

    manual_tahunan = {}
    for bln in range(1, 13):
        d = manual_per_bulan.get(bln, {})
        manual_tahunan[bln] = {
            f: round(sum(d.get(f, []))/len(d[f]), 1) if d.get(f) else None
            for f in ('temp_min', 'temp_max', 'kelembaban', 'kec_angin', 'lama_penyinaran', 'penguapan')
        }

    # ── Data hari ini untuk grafik sehari ──
    today_rd = rdailies_bulan.get(today) or RDaily.select().where(
        RDaily.pos == pos, RDaily.sampling == today
    ).first()
    today_hourly = today_rd._24jam_klimat() if today_rd else {}

    # ── Tabel bulanan ──
    table_data = {}
    for d in range(1, days_in_month + 1):
        tgl = datetime.date(tahun, bulan, d)
        rd  = rdailies_bulan.get(tgl)
        mk  = manuals_bulan.get(tgl)
        tele = None
        if rd:
            try:
                jam_data = rd._24jam_klimat()
                temps  = [v['temperature'] for v in jam_data.values() if v['temperature'] is not None]
                humids = [v['humidity']    for v in jam_data.values() if v['humidity']    is not None]
                winds  = [v['wind_speed']  for v in jam_data.values() if v['wind_speed']  is not None]
                wdirs  = [v['wind_dir']    for v in jam_data.values() if v['wind_dir']    is not None]
                from collections import Counter
                tele = {
                    'suhu':       round(sum(temps)/len(temps), 1)   if temps  else None,
                    'humidity':   round(sum(humids)/len(humids), 1) if humids else None,
                    'wind_speed': round(sum(winds)/len(winds), 1)   if winds  else None,
                    'wind_dir':   Counter(wdirs).most_common(1)[0][0] if wdirs else None,
                }
            except Exception:
                tele = None
        table_data[tgl] = {'tele': tele, 'manual': mk}

    # ── Tabel tahunan ──
    table_tahunan = {}
    for bln in range(1, 13):
        table_tahunan[bln] = {
            'tele':   tele_tahunan.get(bln),
            'manual': manual_tahunan.get(bln),
            'nama_bulan': datetime.date(tahun, bln, 1).strftime('%b')
        }

    NAMA_BULAN = ['Jan','Feb','Mar','Apr','Mei','Jun',
                  'Jul','Ags','Sep','Okt','Nov','Des']

    ctx = {
        'pos': pos,
        'mode': mode,
        'tahun': tahun,
        'bulan': bulan,
        'tahun_list': tahun_list,
        'bulan_list': bulan_list,
        'nama_bulan': NAMA_BULAN,
        'today': today,
        'today_hourly': today_hourly,
        'table_data': table_data,
        'table_tahunan': table_tahunan,
        'tele_tahunan': tele_tahunan,
        'manual_tahunan': manual_tahunan,
        'tele_per_hari': {
            tgl.strftime('%d'): d['tele']
            for tgl, d in table_data.items() if d['tele']
        },
        'manual_per_hari': {
            tgl.strftime('%d'): {
                f: getattr(d['manual'], f)
                for f in ('temp_min','temp_max','kelembaban','kec_angin','lama_penyinaran','penguapan')
            }
            for tgl, d in table_data.items() if d['manual']
        },
    }
    return render_template('pklimat/show.html', ctx=ctx)