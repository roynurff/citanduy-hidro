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

    # Pos-pos pada sungai yang sama
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

    if len(rds):
        try:
            pos.vendor = VENDORS[rds[0].source].get('nama')
        except:
            pass
        wlevels = reduce((lambda x, y: x + y), [json.loads(r.raw) for r in rds])
        df_wlevel = pd.DataFrame(wlevels)
        # Convert wlevel to numeric to avoid mixed type errors
        df_wlevel['wlevel'] = pd.to_numeric(df_wlevel['wlevel'], errors='coerce')
        df_wlevel.set_index('sampling', inplace=True)
        df_wlevel.index = pd.to_datetime(df_wlevel.index)
        desc = df_wlevel.describe()
        if desc.max().wlevel:
            telemetri_obj.max = '{:.1f}'.format(desc.max().wlevel)
            telemetri_obj.min = '{:.1f}'.format(desc.min().wlevel)
        df_wmean = df_wlevel['wlevel'].resample('1h').mean().to_frame(name='wlevel')

        # data 'wlevel' Luwes dijadikan CentiMeter
        if rds[0].source in ('SB', 'SC'):
            df_wmean = df_wmean.mul({'wlevel': 100}) 
        #df_wmax = df_wlevel.resample('1h').max()
        #df_wmin = df_wlevel.resample('1h').min()
        
        fig.add_trace(go.Scatter(x=df_wmean.index, y=df_wmean['wlevel'], mode='lines', name='Telemetri'))

        table_data = df_wmean.to_html(classes="table table-bordered table-striped")
    pos.telemetri = telemetri_obj
    if len(manuals):
        manuals = reduce((lambda x, y: x + y), [m for m in manuals])
        df_man = pd.DataFrame([{'sampling': m[0], 'wlevel': m[1]} for m in manuals])
        
        fig.add_trace(go.Scatter(x=df_man['sampling'], y=df_man['wlevel'], mode='lines', name='Manual'))
    
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
    }
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
    l_debits = dict([(l.pos.id, l) for l in LengkungDebit.select()])
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
                if raw.get('wlevel'):
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
                        ld = l_debits[p.id]
                        raw = json.loads(rdailies[p.id].raw)[-1]
                        p.latest_sampling = raw.get('sampling')
                        if raw.get('wlevel'):
                            wlevel_val = float(raw.get('wlevel'))
                            if p.source == 'SA':
                                p.latest_tma = int(wlevel_val)
                            else:
                                p.latest_tma = int(wlevel_val * 100)
                            p.debit = ld.c_ * ((p.latest_tma * 0.01) + ld.a_) ** ld.b_
                    except (ValueError, TypeError, ZeroDivisionError):
                        p.debit = None
            except Exception as e:
                # Skip this pos if any error occurs
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