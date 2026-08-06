from flask import Blueprint, render_template, request, abort, url_for
from flask_login import login_required
from peewee import DoesNotExist
import pandas as pd
import plotly.graph_objs as go
import plotly.io as pio
import json
import datetime
from types import SimpleNamespace
from functools import reduce

from app.models import Pos, ManualDaily, RDaily, PosMap, VENDORS, Notes, LengkungDebit
from app.forms import NoteForm
from app import get_sampling
bp = Blueprint('pda', __name__, url_prefix='/pda')

PDAPCH = {
    6: (44, 19), # bojongsalawe_6: ciamis_44, janggala_19
    5: (52, 56, 19, 66), # binangun_5: cineam_52, gnputri_56, janggala_19, sidamulih_66
    31: (45, 20, 58, 62, 63, 64, 65), # bandaruka_31: cibariwal_45, danasari_20, kawali_58, panawangan_62, panjalu_63, rancah_64, sadananya_65
    1: (20, 64, 67), # batununggul_1: danasari_20, rancah_64, tanjungjaya_67
    2: (54, 55, 30, 67), # bebedahan_2: dayeuhluhur_54, gnbabakan_55, kaso_30, tanjungjaya_67
    3: (86, 50, 56, 25, 66, 60), # bdciputrahaji_3: ciawitali_86, cikupa_50, gnputri_56, pdaherang_25, padaringan_60, sidamulih_66
    7: (44, 45, 20, 63, 65), # bunar_7: ciamis_44, cibariwal_45, danasari_20, panjalu_63, sadananya_65
    32: (20, 58, 64, 67), # bunter_32: danasari_20, kawali_58, rancah_64, tanjungjaya_67
    
}

def hitung_debit(h, segs):
    '''h dalam meter, segs = list LengkungDebit terurut h_min asc'''
    if not segs:
        return None
    match = None
    for s in segs:
        if (s.h_min is None or h >= s.h_min) and (s.h_max is None or h <= s.h_max):
            match = s
    if match is None:
        match = segs[0]
    try:
        return match.c_ * (h + match.a_) ** match.b_
    except (ValueError, ZeroDivisionError):
        return None

@bp.route('/debit/<int:id>/<int:tahun>/<int:bulan>')
@login_required
def show_debit_month(id, tahun, bulan):
    try:
        pos = Pos.get(id)
    except DoesNotExist:
        return abort(404)

    segs = list(LengkungDebit.select().where(LengkungDebit.pos==pos).order_by(
        LengkungDebit.versi.desc(), LengkungDebit.h_min))
    if not segs:
        abort(404)  # pos ini belum punya rumus lengkung debit
    max_versi = max(s.versi for s in segs)
    segs = [s for s in segs if s.versi == max_versi]

    samp = "{}-{}-1".format(tahun, bulan)
    (_sampling, sampling, sampling_) = get_sampling(samp)
    _sampling = sampling - datetime.timedelta(days=2)
    if sampling.strftime('%Y%m') >= datetime.date.today().strftime('%Y%m'):
        sampling_ = None
    else:
        sampling_ = (sampling + datetime.timedelta(days=32)).replace(day=1)

    rds = RDaily.select(RDaily.raw, RDaily.source).where(RDaily.pos_id==pos.id,
                                RDaily.sampling.year==sampling.year,
                                RDaily.sampling.month==sampling.month).order_by(RDaily.sampling)

    fig = go.Figure()
    fig.update_layout(title='Debit Harian {}'.format(pos.nama.replace('PDA ', '')),
                    xaxis_title='Tanggal',
                    yaxis_title='Debit (m³/dt)',
                    template='plotly_white',
                    yaxis=dict(fixedrange=True, showgrid=True, zeroline=True,
                               gridcolor='LightGray', zerolinecolor='LightGray'))

    daily_rows = []
    rata2 = None
    total_aliran = None

    if len(rds):
        wlevels = reduce((lambda x, y: x + y), [json.loads(r.raw) for r in rds])
        df = pd.DataFrame(wlevels)
        df['wlevel'] = pd.to_numeric(df['wlevel'], errors='coerce')
        df.set_index('sampling', inplace=True)
        df.index = pd.to_datetime(df.index)
        if rds[0].source in ('SB', 'SC'):
            df['wlevel'] = df['wlevel'] * 100  # jadi centimeter, konsisten dg source lain
        df_daily = df['wlevel'].resample('1D').mean().to_frame(name='wlevel')
        df_daily['h'] = df_daily['wlevel'] / 100.0  # ke meter
        df_daily['debit'] = df_daily['h'].apply(lambda h: hitung_debit(h, segs) if pd.notna(h) else None)

        fig.add_trace(go.Scatter(x=df_daily.index, y=df_daily['debit'], mode='lines', name='Debit'))

        valid = df_daily['debit'].dropna()
        if len(valid):
            rata2 = valid.mean()
            total_aliran = valid.sum() * 86400 / 1_000_000  # m3/dt -> juta m3 per bulan

        for tgl, row in df_daily.iterrows():
            daily_rows.append({
                'tanggal': tgl.strftime('%-d %b') if hasattr(tgl, 'strftime') else str(tgl),
                'debit': '{:.2f}'.format(row['debit']) if pd.notna(row['debit']) else '-'
            })

    graph_json = pio.to_json(fig)
    ctx = {
        'pos': pos,
        'sampling': sampling,
        '_sampling': _sampling,
        'sampling_': sampling_,
        'graph': graph_json,
        'rata2': '{:.2f}'.format(rata2) if rata2 is not None else '-',
        'total_aliran': '{:.2f}'.format(total_aliran) if total_aliran is not None else '-',
        'daily_rows': daily_rows,
    }
    return render_template('pda/debit_month.html', ctx=ctx)

@bp.route('/debit/<int:id>/<int:tahun>')
@login_required
def show_debit_year(id, tahun):
    try:
        pos = Pos.get(id)
    except DoesNotExist:
        return abort(404)

    segs = list(LengkungDebit.select().where(LengkungDebit.pos==pos).order_by(
        LengkungDebit.versi.desc(), LengkungDebit.h_min))
    if not segs:
        abort(404)
    max_versi = max(s.versi for s in segs)
    segs = [s for s in segs if s.versi == max_versi]

    tahun_ = tahun + 1 if tahun < datetime.date.today().year else None
    _tahun = tahun - 1

    rds = RDaily.select(RDaily.raw, RDaily.source).where(RDaily.pos_id==pos.id,
                                RDaily.sampling.year==tahun).order_by(RDaily.sampling)

    fig = go.Figure()
    fig.update_layout(title='Debit Bulanan {} - {}'.format(pos.nama.replace('PDA ', ''), tahun),
                    xaxis_title='Bulan',
                    yaxis_title='Debit (m³/dt)',
                    template='plotly_white',
                    yaxis=dict(fixedrange=True, showgrid=True, zeroline=True,
                               gridcolor='LightGray', zerolinecolor='LightGray'))

    monthly_rows = []
    rata2 = None
    total_aliran = None

    if len(rds):
        wlevels = reduce((lambda x, y: x + y), [json.loads(r.raw) for r in rds])
        df = pd.DataFrame(wlevels)
        df['wlevel'] = pd.to_numeric(df['wlevel'], errors='coerce')
        df.set_index('sampling', inplace=True)
        df.index = pd.to_datetime(df.index)
        if rds[0].source in ('SB', 'SC'):
            df['wlevel'] = df['wlevel'] * 100
        df_month = df['wlevel'].resample('1ME').mean().to_frame(name='wlevel')
        df_month['h'] = df_month['wlevel'] / 100.0
        df_month['debit'] = df_month['h'].apply(lambda h: hitung_debit(h, segs) if pd.notna(h) else None)

        fig.add_trace(go.Scatter(x=df_month.index, y=df_month['debit'], mode='lines+markers', name='Debit'))

        valid = df_month['debit'].dropna()
        if len(valid):
            rata2 = valid.mean()
            hari_per_bulan = df_month.index.days_in_month
            total_aliran = (df_month['debit'].fillna(0) * hari_per_bulan * 86400 / 1_000_000).sum()

        for bln, row in df_month.iterrows():
            monthly_rows.append({
                'bulan': bln.strftime('%b'),
                'debit': '{:.2f}'.format(row['debit']) if pd.notna(row['debit']) else '-'
            })

    graph_json = pio.to_json(fig)
    ctx = {
        'pos': pos,
        'tahun': tahun,
        '_tahun': _tahun,
        'tahun_': tahun_,
        'graph': graph_json,
        'rata2': '{:.2f}'.format(rata2) if rata2 is not None else '-',
        'total_aliran': '{:.2f}'.format(total_aliran) if total_aliran is not None else '-',
        'monthly_rows': monthly_rows,
    }
    return render_template('pda/debit_year.html', ctx=ctx)

@bp.route('/<int:id>/<int:tahun>')
def show_year(id, tahun):
    try:
        pos = Pos.get(id)
    except DoesNotExist:
        abort(404)
    samp = "{}-1-1".format(tahun)
    try:
        pm = PosMap.get(PosMap.pos==pos)
        nama = pm.nama
    except DoesNotExist:
        nama = None
    ctx = {
        'pos': pos
    }
    return render_template('pda/year.html', ctx=ctx)

@bp.route('/<int:id>/<int:tahun>/<int:bulan>')
@login_required
def show_month(id, tahun, bulan):
    try:
        pos = Pos.get(id)
    except DoesNotExist:
        return abort(404)
    try:
        pchs = Pos.select().where(Pos.id.in_(PDAPCH[pos.id]))
    except KeyError:
        pchs = []
    samp = "{}-{}-1".format(tahun, bulan)

    (_sampling, sampling, sampling_) = get_sampling(samp)
    _sampling = sampling - datetime.timedelta(days=2)
    if sampling.strftime('%Y%m') >= datetime.date.today().strftime('%Y%m'):
        sampling_ = None
    else:
        sampling_ = (sampling + datetime.timedelta(days=32)).replace(day=1)

    try:
        sibling_pos = Pos.select().where(Pos.sungai==pos.sungai, Pos.tipe=='2').order_by(Pos.elevasi.desc())
    except DoesNotExist:
        sibling_pos = []

    rds = RDaily.select(RDaily.raw, RDaily.source).where(RDaily.pos_id==pos.id,
                                RDaily.sampling.year==sampling.year,
                                RDaily.sampling.month==sampling.month).order_by(
                                    RDaily.sampling)
    select_manual = ManualDaily.select(ManualDaily.sampling, ManualDaily.tma).where(ManualDaily.pos_id==pos.id,
                                         ManualDaily.sampling.year==sampling.year,
                                         ManualDaily.sampling.month==sampling.month).order_by(
                                             ManualDaily.sampling
                                         )
    manuals = [[(datetime.datetime.fromisoformat(m.sampling.isoformat()).replace(hour=int(k)), v) for k,v in json.loads(m.tma).items() if k in ('07', '12', '17')] for m in select_manual]
    fig = go.Figure()
    fig.update_layout(title='Tinggi Muka Air {}'.format(sampling.strftime('%b %Y')),
                    xaxis_title='Waktu',
                    yaxis_title='TMA',
                    template='plotly_white',
                    yaxis=dict(fixedrange=True,
                               title='Tinggi Muka Air (cm)',
                               showgrid=True, zeroline=True, gridcolor='LightGray', zerolinecolor='LightGray'),
                    )
    if pos.sh:
        fig.add_hline(y=pos.sh, line_color='rgb(32, 255, 32)')
    if pos.sk:
        fig.add_hline(y=pos.sk, line_color='rgb(214, 193, 54)')
    if pos.sm:
        fig.add_hline(y=pos.sm, line_color='rgb(255, 32, 32)')

    table_data = ''
    pos.vendor = '-'
    telemetri_obj = SimpleNamespace(max='-', min='-')
    days_dict = {}

    if len(rds):
        try:
            pos.vendor = VENDORS[rds[0].source].get('nama')
        except:
            pass
        wlevels = reduce((lambda x, y: x + y), [json.loads(r.raw) for r in rds])
        df_wlevel = pd.DataFrame(wlevels)
        df_wlevel['wlevel'] = pd.to_numeric(df_wlevel['wlevel'], errors='coerce')
        df_wlevel.set_index('sampling', inplace=True)
        df_wlevel.index = pd.to_datetime(df_wlevel.index)
        desc = df_wlevel.describe()
        if desc.max().wlevel:
            telemetri_obj.max = '{:.1f}'.format(desc.max().wlevel)
            telemetri_obj.min = '{:.1f}'.format(desc.min().wlevel)
        df_wmean = df_wlevel['wlevel'].resample('1h').mean().to_frame(name='wlevel')

        if rds[0].source in ('SB', 'SC'):
            df_wmean = df_wmean.mul({'wlevel': 100})

        fig.add_trace(go.Scatter(x=df_wmean.index, y=df_wmean['wlevel'], mode='lines', name='Telemetri'))

        table_data = df_wmean.to_html(classes="table table-bordered table-striped")

        # resample harian buat tabel sidebar
        df_daily = df_wmean['wlevel'].resample('1D').mean().to_frame(name='wlevel')
        df_count = df_wlevel['wlevel'].resample('1D').count().to_frame(name='count')
        for tgl, row in df_daily.iterrows():
            d = tgl.day
            days_dict[d] = {
                'tele': '{:.1f}'.format(row['wlevel']) if pd.notna(row['wlevel']) else '-',
                'count': int(df_count.loc[tgl, 'count']) if tgl in df_count.index else 0,
                'manual': '-'
            }

    pos.telemetri = telemetri_obj

    if len(manuals):
        manuals_flat = reduce((lambda x, y: x + y), [m for m in manuals])
        df_man = pd.DataFrame([{'sampling': m[0], 'wlevel': m[1]} for m in manuals_flat])
        fig.add_trace(go.Scatter(x=df_man['sampling'], y=df_man['wlevel'], mode='lines', name='Manual'))

    for m in select_manual:
        d = m.sampling.day
        tma = json.loads(m.tma)
        vals = [float(v) for k, v in tma.items() if k in ('07', '12', '17') and v]
        if vals and d in days_dict:
            days_dict[d]['manual'] = '{:.1f}'.format(sum(vals) / len(vals))
        elif vals:
            days_dict[d] = {'tele': '-', 'count': 0, 'manual': '{:.1f}'.format(sum(vals) / len(vals))}

    from collections import OrderedDict
    days_sorted = OrderedDict(sorted(days_dict.items()))

    pos.petugas = pos.petugas_set[0].nama if pos.petugas_set else '-'
    graph_json = pio.to_json(fig)
    ctx = {
        'pos': pos,
        'pchs': pchs,
        'sampling': sampling,
        '_sampling': _sampling,
        'sampling_': sampling_,
        'graph': graph_json,
        'mean_table': table_data,
        'sibling_pos': sibling_pos,
        'days': days_sorted,
    }
    pos.punya_debit = LengkungDebit.select().where(LengkungDebit.pos==pos).exists()
    return render_template('pda/month.html', ctx=ctx)

@bp.route('/<int:id>')
@login_required
def show(id):
    try:
        pos = Pos.get(id)
    except DoesNotExist:
        return abort(404)
    form = NoteForm(obj_name="pos", obj_id=id)
    notes = Notes.select().where(Notes.obj_name=='pos', Notes.obj_id==id).order_by(Notes.cdate)
    try:
        pchs = Pos.select().where(Pos.id.in_(PDAPCH[pos.id]))
    except KeyError:
        pchs = []
    rdailies = None
    (_sampling, sampling, sampling_) = get_sampling(request.args.get('s', None))
    try:
        pm = PosMap.select().where(PosMap.pos==pos).first()
        #if pm:
        rdailies = RDaily.select().where(RDaily.pos==pos, 
                                             RDaily.sampling==sampling.strftime('%Y-%m-%d')).first()
    except DoesNotExist:
        pass
    md = ManualDaily.select().where(ManualDaily.pos==pos,
                                    ManualDaily.sampling==sampling.strftime('%Y-%m-%d')).first()
    pos.telemetri = rdailies._24jam() if rdailies else {}
    pos.manual = md and md._tma or {}

    ctx = {
        'pos': pos,
        'pchs': pchs,
        'sampling': sampling,
        'sampling_': sampling_,
        '_sampling': _sampling,
        'form': form,
        'notes': notes
    }
    pos.punya_debit = LengkungDebit.select().where(LengkungDebit.pos==pos).exists()
    return render_template('pda/show.html', ctx=ctx)        

    
@bp.route('/')
def index():
    (_sampling, sampling, sampling_) = get_sampling(request.args.get('s', None))
    pdas = Pos.select().where(Pos.tipe=='2').order_by(Pos.sungai, Pos.elevasi.desc())

    rdailies = dict([(r.pos_id, r) for r in RDaily.select()
                     .where(RDaily.sampling==sampling.strftime('%Y-%m-%d'))])
    mds = dict([(m.pos.id, m.tma) for m in ManualDaily.select().where(
        ManualDaily.sampling==sampling.strftime('%Y-%m-%d'), 
        ManualDaily.tma.is_null(False))])
    l_debits_raw = LengkungDebit.select()
    l_debits = {}
    for l in l_debits_raw:
        l_debits.setdefault(l.pos_id, []).append(l)
    for pos_id, segs in l_debits.items():
        max_versi = max(s.versi for s in segs)
        l_debits[pos_id] = [s for s in segs if s.versi == max_versi]

    for p in pdas:
        if p.id in mds:
            tma = json.loads(mds.get(p.id))
            for k, v in tma.items():
                if k in ('07', '12', '17'):
                    setattr(p, 'm_tma_' + k, '{:.1f}'.format(float(v)))
        if p.id in rdailies:
            try:
                p.source = rdailies[p.id].source
                tma = rdailies[p.id]._tma() if rdailies[p.id].pos_id == p.id else {}
                for k, v in tma.items():
                    jam = str(k).zfill(2)
                    setattr(p, 'tma_' + jam, '{:.1f}'.format(float(v.get('wlevel'))))
                raw = json.loads(rdailies[p.id].raw)[-1]
                p.latest_sampling = raw.get('sampling')
                p.latest_tma = None
                if raw.get('wlevel') is not None:
                    try:
                        wlevel_val = float(raw.get('wlevel'))
                        if p.source == 'SA':
                            p.latest_tma = int(wlevel_val)
                        else:
                            p.latest_tma = int(wlevel_val * 100)
                    except (ValueError, TypeError):
                        p.latest_tma = None
                if p.id in l_debits:
                    try:
                        raw = json.loads(rdailies[p.id].raw)[-1]
                        p.latest_sampling = raw.get('sampling')
                        if raw.get('wlevel') is not None:
                            wlevel_val = float(raw.get('wlevel'))
                            if p.source == 'SA':
                                p.latest_tma = int(wlevel_val)
                            else:
                                p.latest_tma = int(wlevel_val * 100)
                            h = p.latest_tma * 0.01
                            segs = l_debits[p.id]
                            ld = next((s for s in segs
                                       if (s.h_min is None or h >= s.h_min)
                                       and (s.h_max is None or h <= s.h_max)), segs[0])
                            p.debit = ld.c_ * (h + ld.a_) ** ld.b_
                    except (ValueError, TypeError, ZeroDivisionError):
                        p.debit = None
            except Exception as e:
                print(f'Error processing PDA {p.id}: {str(e)}')
    sungai = set([p.sungai for p in pdas])
    ruas = {}
    for s in sungai:
        ruas.update({s: [p for p in pdas if p.sungai==s]})
    canonical_url = url_for('pda.index', _external=True)
    prev_url = url_for('pda.index', s=_sampling.strftime('%Y-%m-%d'), _external=True) if _sampling else None
    next_url = url_for('pda.index', s=sampling_.strftime('%Y-%m-%d'), _external=True) if sampling_ else None
    ctx = {
        'pdas': pdas,
        'sungai': ruas,
        'sampling': sampling,
        '_sampling': _sampling,
        'sampling_': sampling_
    }
    
    return render_template('pda/index.html', 
                           ctx=ctx,
                           canonical_url=canonical_url,
                           prev_url=prev_url,
                           next_url=next_url)