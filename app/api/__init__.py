import json
from flask import Blueprint, request, abort, render_template, jsonify
from peewee import DoesNotExist, fn
from playhouse.shortcuts import model_to_dict
from flask_wtf.csrf import generate_csrf

from app.models import RDaily, VENDORS, Pos, ManualDaily, Incoming
from app import get_sampling, csrf, limiter

bp = Blueprint('api', __name__, url_prefix='/api')
from app.api import pos


@bp.route('/token')
def get_token():
    return jsonify({'token': generate_csrf()})

@bp.route('/sensor', methods=['POST'])
@csrf.exempt
def sensor():
    out = {'ok': False}
    if request.is_json:
        data = request.get_json()
        ua = request.headers.get('User-Agent')
        new_incoming = Incoming.create(user_agent=ua, body=json.dumps(data))
        if new_incoming:
            out = {'ok': True, 'id': new_incoming.id}
            new_incoming.sb_to_daily()
    return jsonify(out)

@bp.route('/sensor/<uuid>')
def sensor_show(uuid):
    try:
        inc = Incoming.get(Incoming.id==uuid)
    except DoesNotExist:
        return jsonify({'ok': False, 'msg': 'Not found'})
    return jsonify(model_to_dict(inc))

@bp.route('/wlevel')
@limiter.limit("1 per 5 minute")
def wlevel():
    '''{pos: manual: telemetri: }'''
    get_newest = not request.args.get('s', None)
    (_s, s, s_) = get_sampling(request.args.get('s', ''))
    
    # Get all water level monitoring stations
    pdas = Pos.select().where(Pos.tipe=='2').order_by(Pos.sungai, Pos.elevasi.desc())
    pids = [p.id for p in pdas]
    
    if not get_newest:
        # Historical data mode - specific date
        _process_historical_data(pdas, pids, s)
    else:
        # Latest data mode - most recent readings
        _process_latest_data(pdas)
    
    return jsonify({
        'meta': {
            'description': 'Tinggi Muka Air semua Pos Duga Air', 
            'sampling': s.strftime('%Y-%m-%d')
        }, 
        'items': [_format_pos_data(p) for p in pdas]
    })


def _process_historical_data(pdas, pids, sampling_date):
    """Process historical telemetry and manual data for a specific date"""
    # Fetch all records for the date
    rd = dict([
        (r.pos_id, r) 
        for r in RDaily.select().where(
            RDaily.sampling == sampling_date.strftime('%Y-%m-%d'), 
            RDaily.pos_id.in_(pids)
        )
    ])
    
    md = dict([
        (m.pos_id, m) 
        for m in ManualDaily.select().where(
            ManualDaily.sampling == sampling_date.strftime('%Y-%m-%d'),
            ManualDaily.pos_id.in_(pids)
        )
    ])
    
    for p in pdas:
        # Process telemetry data
        try:
            r = rd[p.id]
            t_raw = json.loads(r.raw) if r.raw else []
            
            # Convert units for SB/SC sources (meters to centimeters)
            if t_raw and r.source in ('SB', 'SC'):
                p.telemetri = [
                    {
                        'sampling': item.get('sampling'), 
                        'wlevel': item.get('wlevel') * 100
                    } 
                    for item in t_raw 
                    if item.get('wlevel') is not None
                ]
            else:
                p.telemetri = t_raw
            
            p.vendor = r.vendor
        except KeyError:
            p.telemetri = []
            p.vendor = None
        
        # Process manual data
        p.manual = _extract_manual_data(md.get(p.id))


def _process_latest_data(pdas):
    """Process latest telemetry and manual data for each station"""
    for p in pdas:
        # Get most recent records
        r = p.rdaily_set.order_by(RDaily.sampling.desc()).first()
        m = p.manualdaily_set.order_by(ManualDaily.sampling.desc()).first()
        
        # Process manual data
        p.manual = _extract_manual_data(m)
        
        # Process telemetry data
        if not r or not r.raw:
            p.telemetri = {
                'latest': {},
                'trend': None,
                'raw': []
            }
            p.vendor = None
            continue
        
        t_raw = json.loads(r.raw)
        p.vendor = r.vendor
        
        # Convert units for SB/SC sources BEFORE trend calculation
        is_metric_source = r.source in ('SB', 'SC')
        if is_metric_source:
            t_raw = _convert_to_centimeters(t_raw)
        
        # Calculate trends
        trend_data = _calculate_trends(t_raw, r.source)
        
        # Build telemetry response
        latest_reading = t_raw[-1] if t_raw else {}
        
        p.telemetri = {
            'latest': {
                'sampling': latest_reading.get('sampling'),
                'wlevel': latest_reading.get('wlevel')
            },
            'trend': trend_data,
            'raw': [
                {
                    'sampling': item.get('sampling'), 
                    'wlevel': item.get('wlevel')
                } 
                for item in t_raw 
                if item.get('wlevel') is not None
            ]
        }


def _convert_to_centimeters(t_raw):
    """Convert wlevel from meters to centimeters for metric sources"""
    return [
        {
            'sampling': item.get('sampling'),
            'wlevel': item.get('wlevel') * 100 if item.get('wlevel') is not None else None
        }
        for item in t_raw
    ]


def _calculate_trends(t_raw, source):
    """Calculate water level trends for 15 and 60 minute intervals"""
    if len(t_raw) < 12:
        return {
            "t_15_min": {"wlevel": None, "sampling": None, "trend": None, "delta": None},
            "t_60_min": {"wlevel": None, "sampling": None, "trend": None, "delta": None}
        }
    
    # Determine indices based on source
    # SB sources use different sampling intervals
    if source == 'SB':
        idx_15min = -2  # ~15 min ago
        idx_60min = -5  # ~60 min ago
    else:
        idx_15min = -3  # 15 min ago (5 min intervals)
        idx_60min = -12  # 60 min ago (5 min intervals)
    
    current = t_raw[-1]
    reading_15min = t_raw[idx_15min] if abs(idx_15min) <= len(t_raw) else {}
    reading_60min = t_raw[idx_60min] if abs(idx_60min) <= len(t_raw) else {}
    
    current_wlevel = current.get('wlevel')
    wlevel_15 = reading_15min.get('wlevel')
    wlevel_60 = reading_60min.get('wlevel')
    
    # Calculate deltas and trends
    delta_15 = _calculate_delta(current_wlevel, wlevel_15)
    delta_60 = _calculate_delta(current_wlevel, wlevel_60)
    
    return {
        "t_15_min": {
            "wlevel": wlevel_15,
            "sampling": reading_15min.get('sampling'),
            "trend": _determine_trend(delta_15),
            "delta": delta_15
        },
        "t_60_min": {
            "wlevel": wlevel_60,
            "sampling": reading_60min.get('sampling'),
            "trend": _determine_trend(delta_60),
            "delta": delta_60
        }
    }


def _calculate_delta(current, previous):
    """Calculate delta between current and previous readings"""
    if current is None or previous is None:
        return None
    try:
        return current - previous
    except (TypeError, ValueError):
        return None


def _determine_trend(delta):
    """Determine trend direction from delta value"""
    if delta is None:
        return None
    if delta > 0:
        return "naik"
    elif delta < 0:
        return "turun"
    else:
        return "stabil"


def _extract_manual_data(manual_record):
    """Extract manual readings at 07:00, 12:00, and 17:00"""
    if not manual_record:
        return []
    
    try:
        tma_data = json.loads(manual_record.tma)
        manual_readings = []
        
        for hour in ('07', '12', '17'):
            if hour in tma_data:
                manual_readings.append({
                    'sampling': manual_record.sampling.strftime('%Y-%m-%dT') + hour,
                    'tma': tma_data[hour]
                })
        
        return manual_readings
    except (json.JSONDecodeError, AttributeError):
        return []


def _format_pos_data(p):
    """Format position data for JSON response"""
    return {
        'pos': {
            'nama': p.nama,
            'latlon': p.ll,
            'id': p.id,
            'sungai': p.sungai,
            'elevasi': p.elevasi,
            'sh': p.sh,
            'sk': p.sk,
            'sm': p.sm,
            'kabupaten': p.kabupaten,
            'kecamatan': p.kecamatan,
            'desa': p.desa,
        },
        'telemetri': p.telemetri,
        'vendor': p.vendor,
        'manual': p.manual
    }
    
@bp.route('/rain')
@limiter.limit("5 per minute")
@cache.cached(timeout=300) # Cache for 5 minutes saving
def rain():
    get_newest = request.args.get('s', None)
    (_s, s, s_) = get_sampling(request.args.get('s', None))
    rdaily = RDaily.select().where(RDaily.sampling==s.strftime('%Y-%m-%d'))
    mdaily = dict([(m.pos_id, m) for m in ManualDaily.select().where(ManualDaily.sampling==s.strftime('%Y-%m-%d'),
                                        )])
    out = []
    for r in rdaily:
        if not r._rain(): continue
        if not r.pos: continue
        #if r._rain()['rain24'] == 0: continue
        if r.pos and r.pos.tipe not in ('1', '3'): continue
        try:
            manual = mdaily[r.pos.id].ch
        except:
            manual = None
        row = {'pos': {'nama': r.pos and r.pos.nama or r.nama,
                       'id': r.pos and r.pos.id or None,
                       'latlon': r.pos and r.pos.ll or None,
                       'elevasi': r.pos and r.pos.elevasi or None,
                       'kabupaten': r.pos and r.pos.kabupaten or None,
                       'kecamatan': r.pos and r.pos.kecamatan or None,
                       'desa': r.pos and r.pos.desa or None},
               'vendor': VENDORS[r.source],
               'telemetri':  {
                'count24': r._rain()['count24'], 
                'rain24': r._rain()['rain24'], 
                'rain': r._rain()['hourly'], 
                },
               'manual': manual
        }
        out.append(row)
    
    return jsonify({
        'meta': {
            'description': 'Hujan yang terjadi pada pos Hujan dan Klimat', 
            'sampling': s.strftime('%Y-%m-%d')},
        'items': out})


@bp.route('/pch/<int:id>')
def pch_show(id: int):
    try:
        pos = Pos.get(id)
        if pos.tipe not in ('1', '3'): raise DoesNotExist
    except DoesNotExist:
        return jsonify({
            'code': 404,
            'ok': False})
    rd = pos.rdaily_set.limit(1).first()
    max = ManualDaily.select().where(ManualDaily.pos==pos).order_by(ManualDaily.ch.desc()).first()
    min = ManualDaily.select().where(ManualDaily.pos==pos).order_by(ManualDaily.sampling).first()
    count = ManualDaily.select(fn.Count(ManualDaily.id)).scalar()
    yearly = ManualDaily.select(ManualDaily.sampling.year.alias('tahun'), fn.Sum(ManualDaily.ch).alias('ch')).where(ManualDaily.pos==pos).group_by(ManualDaily.sampling.year)
    pos.max = max
    pos.min = min
    pos.count = count
    vendor = VENDORS[rd.source]['nama'] if VENDORS[rd.source] else '-'
    dict_pos = model_to_dict(pos)
    dict_pos.update({
        'vendor': vendor,
        'manual': {
            'max': 
                {'sampling': max.sampling if max else '-', 'ch': max.ch if max else '-'},
            'count': count,
            'first': 
                {'sampling': min.sampling if min else '-', 'ch': min.ch if min else '-'},
            'yearly': [{'tahun': y.tahun, 'ch': y.ch} for y in yearly]
        }
        })
    return jsonify({
        'ok': True,
        'id': id,
        'pos': dict_pos})