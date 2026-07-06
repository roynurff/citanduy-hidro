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
        GET /api/ka/summary?tahun=2026 (Untuk 1 tahun spesifik)
        GET /api/ka/summary (Untuk tarik SEMUA TAHUN)
        """
        # 1. Ambil param tahun, kalau kosong biarin None
        tahun = request.args.get('tahun', type=int)

        # 2. Query (Urutkan dari yang paling lama ke terbaru)
        query = (HasilUjiKualitasAir.select()
                .order_by(HasilUjiKualitasAir.lokasi, HasilUjiKualitasAir.sampling))

        # 3. Jika user minta tahun spesifik, potong pakai .where()
        if tahun:
            query = query.where(HasilUjiKualitasAir.sampling.year == tahun)

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
                # KUNCI GABUNGAN: Tahun + Periode (Contoh: "2024_1", "2025_2")
                tahun_sampling = hu.sampling.year
                key_periode = f"{tahun_sampling}_{hu.periode}"
                
                lokasi_map[hu.lokasi]['periode'][key_periode] = {
                    'id':       hu.id,
                    'sampling': hu.sampling.isoformat(),
                    'pi':       hu.pi,
                    'status':   hu.status_hasil_uji,
                    'foto_url': (
                        f"https://sihka.bbwscitanduy.id/static/ka/_{tahun_sampling}/_{hu.sampling.strftime('%m')}/{hu.foto_path}"
                        if hu.foto_path else None
                    )
                }

        return jsonify({
            'ok':    True,
            'tahun': tahun if tahun else "Semua Tahun",
            'count': len(lokasi_map),
            'data':  list(lokasi_map.values())
        })