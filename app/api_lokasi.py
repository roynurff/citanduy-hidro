"""
API endpoints untuk manajemen Lokasi Master
"""
from flask import Blueprint, request, jsonify
from flask_login import login_required
from app.models import LokasiMaster
import peewee as pw

bp = Blueprint('api_lokasi', __name__, url_prefix='/api/lokasi')

@bp.route('/list', methods=['GET'])
@login_required
def get_lokasi_list():
    """Get all lokasi dari LokasiMaster dengan optional search"""
    search = request.args.get('search', '').strip().lower()
    
    query = LokasiMaster.select().order_by(LokasiMaster.nama_lokasi)
    
    if search:
        query = query.where(
            LokasiMaster.nama_lokasi.contains(search)
        )
    
    result = [
        {
            'id': loc.id,
            'nama_lokasi': loc.nama_lokasi,
            'sungai': loc.sungai,
            'kota_kabupaten': loc.kota_kabupaten,
            'koordinat': loc.koordinat,
            'text': loc.nama_lokasi,  # For Select2
            'value': loc.id  # For Select2
        }
        for loc in query
    ]
    
    return jsonify({'results': result})


@bp.route('/get/<int:lokasi_id>', methods=['GET'])
@login_required
def get_lokasi_detail(lokasi_id):
    """Get detail lokasi berdasarkan ID untuk auto-fill form"""
    try:
        lokasi = LokasiMaster.get_by_id(lokasi_id)
        return jsonify({
            'ok': True,
            'id': lokasi.id,
            'nama_lokasi': lokasi.nama_lokasi,
            'sungai': lokasi.sungai,
            'kota_kabupaten': lokasi.kota_kabupaten,
            'koordinat': lokasi.koordinat
        })
    except LokasiMaster.DoesNotExist:
        return jsonify({'ok': False, 'error': 'Lokasi not found'}), 404


@bp.route('/search', methods=['GET'])
@login_required
def search_lokasi():
    """Search lokasi (for Select2 ajax)"""
    term = request.args.get('term', '').strip().lower()
    
    if not term:
        return jsonify({'results': []})
    
    query = LokasiMaster.select().where(
        LokasiMaster.nama_lokasi.contains(term)
    ).limit(20)
    
    result = [
        {
            'id': loc.id,
            'text': loc.nama_lokasi
        }
        for loc in query
    ]
    
    return jsonify({'results': result})


@bp.route('/create', methods=['POST'])
@login_required
def create_lokasi():
    """Create new lokasi (untuk add new di Select2)"""
    data = request.get_json()
    
    if not data or 'nama_lokasi' not in data:
        return jsonify({'ok': False, 'error': 'nama_lokasi required'}), 400
    
    nama_lokasi = data.get('nama_lokasi', '').strip()
    
    if not nama_lokasi:
        return jsonify({'ok': False, 'error': 'nama_lokasi cannot be empty'}), 400
    
    # Check if already exists
    if LokasiMaster.select().where(LokasiMaster.nama_lokasi == nama_lokasi).exists():
        return jsonify({'ok': False, 'error': 'Lokasi sudah exist'}), 409
    
    try:
        lokasi = LokasiMaster.create(
            nama_lokasi=nama_lokasi,
            sungai=data.get('sungai'),
            kota_kabupaten=data.get('kota_kabupaten'),
            koordinat=data.get('koordinat')
        )
        
        return jsonify({
            'ok': True,
            'id': lokasi.id,
            'nama_lokasi': lokasi.nama_lokasi,
            'sungai': lokasi.sungai,
            'kota_kabupaten': lokasi.kota_kabupaten,
            'koordinat': lokasi.koordinat
        }), 201
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 400


@bp.route('/update/<int:lokasi_id>', methods=['POST'])
@login_required
def update_lokasi(lokasi_id):
    """Update lokasi data"""
    data = request.get_json()
    
    try:
        lokasi = LokasiMaster.get_by_id(lokasi_id)
        
        # Update fields
        if 'nama_lokasi' in data:
            lokasi.nama_lokasi = data['nama_lokasi']
        if 'sungai' in data:
            lokasi.sungai = data['sungai']
        if 'kota_kabupaten' in data:
            lokasi.kota_kabupaten = data['kota_kabupaten']
        if 'koordinat' in data:
            lokasi.koordinat = data['koordinat']
        
        lokasi.save()
        
        return jsonify({
            'ok': True,
            'id': lokasi.id,
            'nama_lokasi': lokasi.nama_lokasi,
            'sungai': lokasi.sungai,
            'kota_kabupaten': lokasi.kota_kabupaten,
            'koordinat': lokasi.koordinat
        })
    except LokasiMaster.DoesNotExist:
        return jsonify({'ok': False, 'error': 'Lokasi not found'}), 404
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 400


@bp.route('/delete/<int:lokasi_id>', methods=['POST'])
@login_required  
def delete_lokasi(lokasi_id):
    """Delete lokasi (dengan confirmation)"""
    try:
        lokasi = LokasiMaster.get_by_id(lokasi_id)
        
        # Check if ada HasilUjiKualitasAir records yang reference lokasi ini
        from app.models import HasilUjiKualitasAir
        count = HasilUjiKualitasAir.select().where(
            HasilUjiKualitasAir.lokasi_master_id == lokasi_id
        ).count()
        
        if count > 0:
            return jsonify({
                'ok': False,
                'error': f'Tidak bisa delete lokasi "{lokasi.nama_lokasi}" karena ada {count} data kualitas air yang menggunakan lokasi ini. Hapus data tersebut terlebih dahulu.'
            }), 409
        
        lokasi_nama = lokasi.nama_lokasi
        lokasi.delete_instance()
        
        return jsonify({
            'ok': True,
            'message': f'Lokasi "{lokasi_nama}" berhasil dihapus'
        })
    except LokasiMaster.DoesNotExist:
        return jsonify({'ok': False, 'error': 'Lokasi not found'}), 404
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 400
