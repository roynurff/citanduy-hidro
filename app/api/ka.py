import datetime
from flask import request, jsonify
from app.models import HasilUjiKualitasAir, LokasiMaster, ParameterDetail

def register_routes(bp):

    @bp.route('/ka/list')
    def ka_list():
        """
        GET /api/ka/list?tahun=2026
        GET /api/ka/list?tahun=2026&lokasi=Leuwikeris
        GET /api/ka/list?tahun=2026&periode=1
        """
        tahun   = request.args.get('tahun', datetime.date.today().year, type=int)
        lokasi  = request.args.get('lokasi', None)
        periode = request.args.get('periode', None, type=int)

        query = (HasilUjiKualitasAir.select()
                 .where(HasilUjiKualitasAir.sampling.year == tahun))

        if lokasi:
            query = query.where(HasilUjiKualitasAir.lokasi == lokasi)
        if periode:
            query = query.where(HasilUjiKualitasAir.periode == periode)

        query = query.order_by(HasilUjiKualitasAir.lokasi, HasilUjiKualitasAir.sampling)

        results = []
        for hu in query:
            results.append({
                'id':              hu.id,
                'lokasi':          hu.lokasi,
                'sungai':          hu.sungai,
                'kota_kabupaten':  hu.kota_kabupaten,
                'koordinat':       hu.ll,
                'sampling':        hu.sampling.isoformat(),
                'tahun':           hu.sampling.year,
                'periode':         hu.periode,
                'pi':              hu.pi,
                'status':          hu.status_hasil_uji,
                'kelas_baku_mutu': hu.kelas_baku_mutu,
                'lembaga':         hu.lembaga,
            })

        return jsonify({
            'ok':    True,
            'tahun': tahun,
            'count': len(results),
            'data':  results
        })


    @bp.route('/ka/<int:record_id>')
    def ka_detail(record_id):
        """GET /api/ka/<id> — detail satu record + semua parameter"""
        try:
            hu = HasilUjiKualitasAir.get_by_id(record_id)
        except HasilUjiKualitasAir.DoesNotExist:
            return jsonify({'ok': False, 'error': 'Not found'}), 404

        params = list(ParameterDetail.select()
                      .where(ParameterDetail.hasil_uji == hu))

        return jsonify({
            'ok': True,
            'data': {
                'id':              hu.id,
                'lokasi':          hu.lokasi,
                'sungai':          hu.sungai,
                'kota_kabupaten':  hu.kota_kabupaten,
                'koordinat':       hu.ll,
                'sampling':        hu.sampling.isoformat(),
                'tahun':           hu.sampling.year,
                'periode':         hu.periode,
                'pi':              hu.pi,
                'status':          hu.status_hasil_uji,
                'kelas_baku_mutu': hu.kelas_baku_mutu,
                'lembaga':         hu.lembaga,
                'keterangan':      hu.keterangan,
                'parameters': [
                    {
                        'parameter_name': p.parameter_name,
                        'satuan':         p.satuan,
                        'nilai':          p.nilai,
                    }
                    for p in params
                ]
            }
        })


    @bp.route('/ka/lokasi')
    def ka_lokasi():
        """GET /api/ka/lokasi — semua lokasi master"""
        lokasi_list = LokasiMaster.select().order_by(LokasiMaster.nama_lokasi)
        return jsonify({
            'ok': True,
            'data': [
                {
                    'id':             loc.id,
                    'nama_lokasi':    loc.nama_lokasi,
                    'sungai':         loc.sungai,
                    'kota_kabupaten': loc.kota_kabupaten,
                    'koordinat':      loc.koordinat,
                }
                for loc in lokasi_list
            ]
        })


    @bp.route('/ka/summary')
    def ka_summary():
        """
        GET /api/ka/summary?tahun=2026
        Ringkasan per lokasi per periode — cocok untuk tabel SIH3
        """
        tahun = request.args.get('tahun', datetime.date.today().year, type=int)

        query = (HasilUjiKualitasAir.select()
                 .where(HasilUjiKualitasAir.sampling.year == tahun)
                 .order_by(HasilUjiKualitasAir.lokasi, HasilUjiKualitasAir.periode))

        lokasi_map = {}
        for hu in query:
            if hu.lokasi not in lokasi_map:
                lokasi_map[hu.lokasi] = {
                    'lokasi':         hu.lokasi,
                    'sungai':         hu.sungai,
                    'kota_kabupaten': hu.kota_kabupaten,
                    'koordinat':      hu.ll,
                    'periode':        {}
                }
            if hu.periode:
                lokasi_map[hu.lokasi]['periode'][hu.periode] = {
                    'id':       hu.id,
                    'sampling': hu.sampling.isoformat(),
                    'pi':       hu.pi,
                    'status':   hu.status_hasil_uji,
                }

        return jsonify({
            'ok':    True,
            'tahun': tahun,
            'count': len(lokasi_map),
            'data':  list(lokasi_map.values())
        })