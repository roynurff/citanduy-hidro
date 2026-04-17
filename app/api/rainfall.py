from flask import Blueprint, request, jsonify, abort
from peewee import fn
import datetime
from app.models import ManualDaily, Pos

bp = Blueprint('rainfall', __name__, url_prefix='/api')


@bp.route('/rainfall/manual')
def get_manual_rainfall():
    """
    GET /api/rainfall/manual
    
    Endpoint untuk mengambil data curah hujan manual yang diinput oleh petugas
    
    Optional query parameters:
    - date: Tanggal spesifik (format: YYYY-MM-DD, default: today)
    - pos_id: ID pos tertentu (bisa multiple: ?pos_id=1,2,3)
    - type: Filter berdasarkan tipe pos (1=PCH, 2=PDA, 3=Climate)
    
    Returns:
    {
      "meta": {
        "query_date": "2024-01-15",
        "total_pch_climate": 50,
        "data_filled": 45,
        "data_missing": 5
      },
      "items": [
        {
          "pos_id": 15,
          "pos_name": "PCH Situgede",
          "latitude": -7.2345,
          "longitude": 108.5678,
          "pos_type": "1",
          "elevation": 450,
          "district": "Ciamis",
          "rainfall_24h": 12.5,
          "unit": "mm",
          "sampling_date": "2024-01-15",
          "input_by": "petugas_name",
          "input_time": "2024-01-15T15:30:45",
          "data_status": "filled"
        },
        {
          "pos_id": 16,
          "pos_name": "PCH Cipanas",
          "latitude": -7.3456,
          "longitude": 108.6789,
          "pos_type": "1",
          "elevation": 480,
          "district": "Ciamis",
          "rainfall_24h": None,
          "unit": "mm",
          "sampling_date": "2024-01-15",
          "input_by": None,
          "input_time": None,
          "data_status": "missing"
        }
      ]
    }
    """
    try:
        # Parse date parameter (default: today)
        date_str = request.args.get('date', datetime.date.today().isoformat())
        sampling_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        return jsonify({'ok': False, 'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
    
    # Parse pos_id filter (bisa multiple)
    pos_ids = None
    pos_id_param = request.args.get('pos_id')
    if pos_id_param:
        try:
            pos_ids = [int(x.strip()) for x in pos_id_param.split(',')]
        except ValueError:
            return jsonify({'ok': False, 'error': 'Invalid pos_id format. Use comma-separated integers'}), 400
    
    # Parse type filter
    pos_types = None
    type_param = request.args.get('type')
    if type_param:
        pos_types = [x.strip() for x in type_param.split(',')]
    
    # Get all PCH and Climate stations for missing data calculation
    base_query = Pos.select().where(Pos.tipe.in_(('1', '3')))
    
    if pos_ids:
        base_query = base_query.where(Pos.id.in_(pos_ids))
    
    if pos_types:
        base_query = base_query.where(Pos.tipe.in_(pos_types))
    
    all_stations = list(base_query.order_by(Pos.nama))
    
    # Query manual rainfall data untuk sampling date
    manual_query = (ManualDaily
                    .select(ManualDaily, Pos)
                    .join(Pos)
                    .where(ManualDaily.sampling == sampling_date))
    
    if pos_ids:
        manual_query = manual_query.where(Pos.id.in_(pos_ids))
    
    if pos_types:
        manual_query = manual_query.where(Pos.tipe.in_(pos_types))
    
    # Build dictionary of filled data for quick lookup
    filled_data = {}
    for md in manual_query:
        filled_data[md.pos_id] = md
    
    # Build response with both filled and missing data
    items = []
    
    for station in all_stations:
        if station.id in filled_data:
            # Data filled
            md = filled_data[station.id]
            try:
                lat, lon = [float(x.strip()) for x in station.ll.split(',')]
            except (ValueError, TypeError, AttributeError):
                lat, lon = None, None
            
            item = {
                'pos_id': station.id,
                'pos_name': station.nama,
                'latitude': lat,
                'longitude': lon,
                'pos_type': station.tipe,
                'elevation': station.elevasi,
                'district': station.kabupaten,
                'rainfall_24h': md.ch,
                'unit': 'mm',
                'sampling_date': md.sampling.isoformat(),
                'input_by': md.username,
                'input_time': md.cdate.isoformat(),
                'data_status': 'filled'
            }
            items.append(item)
        else:
            # Data missing
            try:
                lat, lon = [float(x.strip()) for x in station.ll.split(',')]
            except (ValueError, TypeError, AttributeError):
                lat, lon = None, None
            
            item = {
                'pos_id': station.id,
                'pos_name': station.nama,
                'latitude': lat,
                'longitude': lon,
                'pos_type': station.tipe,
                'elevation': station.elevasi,
                'district': station.kabupaten,
                'rainfall_24h': None,
                'unit': 'mm',
                'sampling_date': sampling_date.isoformat(),
                'input_by': None,
                'input_time': None,
                'data_status': 'missing'
            }
            items.append(item)
    
    # Calculate statistics
    filled_count = len(filled_data)
    missing_count = len(all_stations) - filled_count
    
    response = {
        'ok': True,
        'meta': {
            'query_date': sampling_date.isoformat(),
            'total_pch_climate': len(all_stations),
            'data_filled': filled_count,
            'data_missing': missing_count,
            'filters': {
                'pos_ids': pos_ids,
                'types': pos_types
            }
        },
        'items': items
    }
    
    return jsonify(response)


@bp.route('/rainfall/manual/stats')
def get_manual_rainfall_stats():
    """
    GET /api/rainfall/manual/stats
    
    Endpoint untuk mengambil statistik curah hujan dalam periode tertentu
    
    Optional query parameters:
    - start_date: Tanggal mulai (format: YYYY-MM-DD)
    - end_date: Tanggal akhir (format: YYYY-MM-DD, default: today)
    - pos_id: ID pos tertentu
    
    Returns:
    {
      "meta": {
        "period": "2024-01-01 to 2024-01-31",
        "total_days_in_period": 31,
        "sampling_dates_with_data": 28
      },
      "items": [
        {
          "pos_id": 15,
          "pos_name": "PCH Situgede",
          "total_rainfall": 156.5,
          "avg_daily_rainfall": 5.6,
          "rainy_days": 15,
          "max_daily_rainfall": 28.5,
          "min_daily_rainfall": 0.1
        }
      ]
    }
    """
    try:
        end_date = datetime.datetime.strptime(
            request.args.get('end_date', datetime.date.today().isoformat()), 
            '%Y-%m-%d'
        ).date()
        
        # Default to 30 days back if no start_date provided
        start_date_str = request.args.get('start_date')
        if start_date_str:
            start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date()
        else:
            start_date = end_date - datetime.timedelta(days=30)
    except ValueError:
        return jsonify({'ok': False, 'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
    
    # Parse pos_id
    pos_id = request.args.get('pos_id')
    if pos_id:
        try:
            pos_id = int(pos_id)
        except ValueError:
            return jsonify({'ok': False, 'error': 'Invalid pos_id'}), 400
    
    # Query data dalam range
    query = (ManualDaily
             .select(ManualDaily, Pos)
             .join(Pos)
             .where(
                 ManualDaily.sampling.between(start_date, end_date),
                 Pos.tipe.in_(('1', '3'))
             ))
    
    if pos_id:
        query = query.where(Pos.id == pos_id)
    
    # Group by pos
    stats_dict = {}
    for md in query:
        if md.pos_id not in stats_dict:
            stats_dict[md.pos_id] = {
                'pos': md.pos,
                'data': []
            }
        if md.ch is not None:
            stats_dict[md.pos_id]['data'].append(md.ch)
    
    # Build response
    items = []
    for pos_id, stat_data in stats_dict.items():
        pos = stat_data['pos']
        rainfall_values = stat_data['data']
        
        if rainfall_values:
            total_rainfall = sum(rainfall_values)
            rainy_days = sum(1 for r in rainfall_values if r > 0)
            days_count = len(rainfall_values)
            
            item = {
                'pos_id': pos.id,
                'pos_name': pos.nama,
                'total_rainfall': round(total_rainfall, 1),
                'avg_daily_rainfall': round(total_rainfall / days_count, 1),
                'rainy_days': rainy_days,
                'max_daily_rainfall': round(max(rainfall_values), 1),
                'min_daily_rainfall': round(min(rainfall_values), 1),
                'data_points': days_count
            }
            items.append(item)
    
    num_days = (end_date - start_date).days + 1
    
    response = {
        'ok': True,
        'meta': {
            'period': f'{start_date.isoformat()} to {end_date.isoformat()}',
            'total_days_in_period': num_days,
            'stations_with_data': len(items)
        },
        'items': items
    }
    
    return jsonify(response)
