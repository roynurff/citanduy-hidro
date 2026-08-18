import datetime
from collections import defaultdict
from types import SimpleNamespace
from flask import Blueprint, render_template, url_for, abort, request
from flask_login import login_required, current_user
from peewee import DoesNotExist, fn

from app.models import Pos, RDaily, ManualDaily, PosMap, VENDORS, Notes
from app.forms import NoteForm
from app import get_sampling
bp = Blueprint('pch', __name__, url_prefix='/pch')

PCH_HIDE = (81,59,55)

wilayah_adm = 'ciamis_tasikmalaya_kota tasikmalaya_kuningan_kota banjar_pangandaran_cilacap_banyumas'.split('_')

@bp.route('/<int:id>/<int:tahun>')
@login_required
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
    (_sampling, sampling, sampling_) = get_sampling(samp)
    _sampling = sampling.replace(year=sampling.year - 1)
    sampling_ = sampling.replace(year=sampling.year + 1)
    if datetime.date.today().year == sampling.year:
        sampling_ = None
    all_years = (ManualDaily.select(ManualDaily.sampling.year.alias('tahun'), 
                                    ManualDaily.sampling.month.alias('bulan'), 
                                    fn.Sum(ManualDaily.ch).alias('ch'),
                                    fn.Count(ManualDaily.id).alias('banyak'))
                 .where(ManualDaily.pos==pos)
                 .group_by(ManualDaily.sampling.year, ManualDaily.sampling.month)
                 .order_by(ManualDaily.sampling.year, ManualDaily.sampling.month))
    this_year = [ay for ay in all_years if ay.tahun==sampling.year]
    all_year = defaultdict(dict)
    for a in all_years:
        if 'ch' in all_year[a.tahun]:
            all_year[a.tahun]['ch'] += a.ch
        else:
            all_year[a.tahun]['ch'] = a.ch
        if 'banyak' in all_year[a.tahun]:
            all_year[a.tahun]['banyak'] += a.banyak
        else:
            all_year[a.tahun]['banyak'] = a.banyak

    ctx = {
        'pos': pos,
        'this_year': this_year,
        'all_year': dict(all_year),
        'sampling': sampling,
        '_sampling': _sampling,
        'sampling_': sampling_
    }
    return render_template('pch/year.html', ctx=ctx)

@bp.route('/<int:id>/<int:tahun>/<int:bulan>')
@login_required
def show_month(id, tahun, bulan):
    try:
        # Validate input parameters
        if not (1 <= bulan <= 12):
            abort(404, "Invalid month")
        
        # Get position
        pos = Pos.get(id)
    except DoesNotExist:
        abort(404, "Position not found")
    except Exception as e:
        abort(500, f"Database error: {str(e)}")

    # Create sampling date
    try:
        sampling_date = datetime.date(tahun, bulan, 1)
    except ValueError as e:
        abort(404, f"Invalid date: {str(e)}")

    # Get position name
    try:
        pos_map = PosMap.get(PosMap.pos == pos)
        nama = pos_map.nama
    except DoesNotExist:
        nama = None

    # Calculate date ranges
    prev_sampling = sampling_date - datetime.timedelta(days=2)
    
    # Determine next sampling date
    today = datetime.date.today()
    if sampling_date.strftime('%Y%m') >= today.strftime('%Y%m'):
        next_sampling = None
    else:
        next_sampling = (sampling_date + datetime.timedelta(days=32)).replace(day=1)

    # Initialize days dictionary
    days = _initialize_days_dict(sampling_date, next_sampling, today)

    # Fetch and process data
    _populate_rainfall_data(pos, sampling_date, days, nama)

    ctx = {
        '_sampling': prev_sampling,
        'sampling': sampling_date,
        'sampling_': next_sampling,
        'pos': pos,
        'nama': nama,
        'days': days
    }
    
    return render_template('pch/month.html', ctx=ctx)


def _initialize_days_dict(sampling_date, next_sampling, today):
    """Initialize the days dictionary with proper date range."""
    if next_sampling and next_sampling < today:
        num_days = (next_sampling - sampling_date).days
    else:
        num_days = today.day
    
    return {
        day: {'count': 0, 'rain': 0, 'mrain': 0}
        for day in range(1, num_days + 1)
    }


def _populate_rainfall_data(pos, sampling_date, days, nama):
    """Populate rainfall data from both automatic and manual sources."""
    
    # Fetch manual rainfall data
    try:
        manual_data = ManualDaily.select().where(
            ManualDaily.pos == pos,
            ManualDaily.sampling.year == sampling_date.year,
            ManualDaily.sampling.month == sampling_date.month
        )
        
        for manual in manual_data:
            if manual.sampling.day in days:
                days[manual.sampling.day]['mrain'] = manual.ch
    except Exception as e:
        # Log error but don't break the page
        print(f"Error fetching manual data: {e}")

    # Fetch automatic rainfall data if position has a name
    try:
        auto_data = RDaily.select().where(
            RDaily.pos == pos,
            RDaily.sampling.year == sampling_date.year,
            RDaily.sampling.month == sampling_date.month
        )
        
        for auto in auto_data:
            if auto.sampling.day in days:
                rain_data = auto._rain()
                days[auto.sampling.day]['count'] = rain_data.get('count24', 0)
                days[auto.sampling.day]['rain'] = rain_data.get('rain24', 0)
    except Exception as e:
        # Log error but don't break the page
        print(f"Error fetching automatic data: {e}")


@bp.route('/<id>')
@login_required
def show(id):
    form = NoteForm(obj_name="pos", obj_id=id)
    notes = Notes.select().where(Notes.obj_name=='pos', Notes.obj_id==id).order_by(Notes.cdate)
    try:
        pos = Pos.get(int(id))
    except DoesNotExist:
        return abort(404)
    (_sampling, sampling, sampling_) = get_sampling(request.args.get('s', None))
    
    manual_first = ManualDaily.select(fn.Min(ManualDaily.sampling).alias('sampling')).where(ManualDaily.pos==pos).first()
    manual_max = ManualDaily.select(fn.Max(ManualDaily.ch).alias('ch')).where(ManualDaily.pos==pos).scalar()
    
    query_max = (ManualDaily
             .select(ManualDaily.sampling, ManualDaily.ch)
             .where(ManualDaily.ch == manual_max)).first()
    manual_today = ManualDaily.select(ManualDaily.ch).where(ManualDaily.pos==pos, ManualDaily.sampling==sampling.strftime('%Y-%m-%d')).first()
    
    man_max = query_max if query_max is not None else SimpleNamespace(ch=0, sampling=None)
    
    manual = dict(ch=manual_today.ch if manual_today else '0', 
                  max={'ch': int(man_max.ch), 'sampling': man_max.sampling}, 
                  first={'ch': manual_first.ch, 'sampling': manual_first.sampling})
    pos.manual = manual
    '''
    pos.manual.first
    pos.manual.max
    pos.manual.today
    '''
    
    try:
        this_day = RDaily.select().where(RDaily.pos==pos, 
                                         RDaily.sampling == sampling.strftime('%Y-%m-%d')).first()
    except DoesNotExist:
        pass
    
    ctx = {'pos': pos,
           'sampling': sampling,
           '_sampling': _sampling,
           'sampling_': sampling_,
           'this_day': this_day,
           'vendors': VENDORS,
           'form': form,
           'notes': notes
           }
    return render_template('pch/show.html', ctx=ctx)

@bp.route('/')
def index():
    if not request.args.get('s'):
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        (_sampling, sampling, sampling_) = get_sampling(yesterday)
    else:
        (_sampling, sampling, sampling_) = get_sampling(request.args.get('s', None))

    pchs = [p for p in Pos.select().where(Pos.tipe.in_(('1', '3'))).order_by(
        Pos.kabupaten, Pos.elevasi.desc(), Pos.nama) if p.id not in PCH_HIDE]
    
    rdailies = dict([(r.pos.id, r) for r in RDaily.select()
                     .where(RDaily.pos.in_(list([p.id for p in pchs])), 
                            RDaily.sampling==sampling.strftime('%Y-%m-%d'))])
    data_manual = dict([(m.pos.id, m.ch) for m in ManualDaily.select().where(ManualDaily.pos.in_([p for p in pchs]), ManualDaily.sampling==sampling.strftime('%Y-%m-%d'))])

    from app.models import VENDORS
    
    for p in pchs:
        if p.id in data_manual:
            p.m_ch = data_manual[p.id]
        if p.id in rdailies and rdailies[p.id]._rain():
            rd = rdailies[p.id]._rain()
            p.pagi = '{:.1f}'.format(sum([v.get('rain') for k, v in rd.get('hourly').items() if int(k[11:13]) >=7 and int(k[11:13]) < 13]))
            p.siang = '{:.1f}'.format(sum([v.get('rain') for k, v in rd.get('hourly').items() if int(k[11:13]) >=13 and int(k[11:13]) < 19]))
            p.malam = '{:.1f}'.format(sum([v.get('rain') for k, v in rd.get('hourly').items() if int(k[11:13]) >=19 and int(k[11:13]) < 24]) + \
                sum([v.get('rain') for k, v in rd.get('hourly').items() if int(k[11:13]) >=0 and int(k[11:13]) < 1]))
            p.dini = '{:.1f}'.format(sum([v.get('rain') for k, v in rd.get('hourly').items() if int(k[11:13]) >=1 and int(k[11:13]) < 7]))
            p.ch = '{:.1f}'.format(rd.get('rain24'))
            p.count = rd.get('count24') or 0
            p.source = rdailies[p.id].source
            p.vendor = VENDORS.get(rdailies[p.id].source).get('nama')
            p.v_name = rdailies[p.id].nama

            max_count = 288
            if rdailies[p.id].source == 'SB':
                max_count = 96
            p.sehat = '{:.1f}%'.format((p.count / max_count) * 100)
    kabs = set([p.kabupaten for p in pchs])
    wils = {}
    for k in kabs:
        wils.update({k: [p for p in pchs if p.kabupaten == k]})
    canonical_url = url_for('pch.index', _external=True)
    prev_url = url_for('pch.index', s=_sampling.strftime('%Y-%m-%d'), _external=True) if _sampling else None
    next_url = url_for('pch.index', s=sampling_.strftime('%Y-%m-%d'), _external=True) if sampling_ else None
    ctx = {
        '_sampling': _sampling,
        'sampling': sampling,
        'sampling_': sampling_,
        'pchs': pchs,
        'wilayah': wils,
        'kabs': wilayah_adm
    }
    return render_template('pch/index.html', 
                           ctx=ctx, 
                           canonical_url=canonical_url,
                           prev_url=prev_url,
                           next_url=next_url)
