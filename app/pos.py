from flask import Blueprint, render_template, jsonify, request
from flask import abort, redirect, flash, current_app
from flask_login import current_user, login_required
from werkzeug.utils import secure_filename
import os
import json
import datetime
from peewee import DoesNotExist, fn

from app.models import Pos, ManualDaily, PosMap, OPos, LengkungDebit, LuwesPos, HasilUjiKualitasAir, LokasiMaster, ParameterDetail, PARAMETER_LIST
from app import get_sampling
from app.forms import CurahHujanForm, TmaForm, HasilUjiKAForm
from weasyprint import HTML as WeasyprintHTML
bp = Blueprint('pos', __name__, url_prefix='/pos')


@bp.route('/da')
@login_required
def pos_da():
    ctx = {
        'poses': Pos.select().where(Pos.tipe=='2').order_by(Pos.nama)
    }
    return render_template('pos/duga_air.html', ctx=ctx)

@bp.route('/ka/delete/<int:id>', methods=['POST'])
@login_required
def delete_data_ka(id):
    try:
        hu = HasilUjiKualitasAir.get(HasilUjiKualitasAir.id == id)
    except DoesNotExist:
        return jsonify({'ok': False, 'error': 'Not found'}), 404
    
    try:
        # Fix path: jangan double-append doc_path ke path yang sudah ada nama file
        if hu.doc_path:
            rel_dir = f"app/static/ka/{hu.sampling.strftime('_%Y/_%m')}"
            doc_full = os.path.join(rel_dir, hu.doc_path)
            if os.path.exists(doc_full):
                os.remove(doc_full)
        
        if hu.foto_path:
            rel_dir = f"app/static/ka/{hu.sampling.strftime('_%Y/_%m')}"
            foto_full = os.path.join(rel_dir, hu.foto_path)
            if os.path.exists(foto_full):
                os.remove(foto_full)
        
        # Hapus parameter details dulu (FK constraint)
        ParameterDetail.delete().where(ParameterDetail.hasil_uji == hu).execute()
        hu.delete_instance()
        
        return jsonify({'ok': True, 'message': 'Data berhasil dihapus'})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@bp.route('/ka/add', methods=['GET', 'POST'])
@login_required
def add_data_ka():
    form = HasilUjiKAForm()
    
    # Get tahun/year from request args or use current year
    tahun = int(request.args.get('tahun', datetime.date.today().year))
    
    if form.validate_on_submit():
        try:
            # Get form data
            lokasi_id = request.form.get('lokasi_id')
            sungai = form.sungai.data.strip() if form.sungai.data else None
            kota_kabupaten = form.kota_kabupaten.data.strip() if form.kota_kabupaten.data else None
            pi = form.pi.data
            keterangan = form.keterangan.data.strip() if form.keterangan.data else None
            periode = int(form.periode.data)
            sampling = form.sampling.data  # Required DateField
            lembaga = form.lembaga.data.strip() if form.lembaga.data else None
            ll = form.ll.data.strip() if form.ll.data else None
            kelas_baku_mutu = int(form.kelas_baku_mutu.data) if form.kelas_baku_mutu.data else 2
            
            # Get lokasi_master if lokasi_id provided
            lokasi_master = None
            lokasi_name = None
            if lokasi_id and lokasi_id.isdigit():
                lokasi_master = LokasiMaster.get_by_id(int(lokasi_id))
                lokasi_name = lokasi_master.nama_lokasi
                # Override dengan data dari master jika tidak diisi manual
                if not sungai: sungai = lokasi_master.sungai
                if not kota_kabupaten: kota_kabupaten = lokasi_master.kota_kabupaten
                if not ll: ll = lokasi_master.koordinat
            else:
                # Fallback untuk manual input
                lokasi_name = request.form.get('lokasi', 'Unknown')
            
            # Create directory path
            rel_dir = f"{current_app.config['KUALITAS_AIR_FOLDER']}/{sampling.strftime('_%Y/_%m')}"
            full_dir = os.path.join('app', rel_dir)
            os.makedirs(full_dir, exist_ok=True)
            
            # Handle main document (hasil uji lab)
            doc_filename = None
            if 'fname' in request.files and request.files['fname'].filename != '':
                file = request.files['fname']
                doc_filename = secure_filename(file.filename or '')
                full_file_path = os.path.join(full_dir, doc_filename)
                file.save(full_file_path)
            
            # Handle foto dokumentasi
            foto_filename = None
            if 'foto' in request.files and request.files['foto'].filename != '':
                file = request.files['foto']
                foto_filename = secure_filename(file.filename or '')
                full_file_path = os.path.join(full_dir, foto_filename)
                file.save(full_file_path)
            
            # Create record
            hu = HasilUjiKualitasAir.create(
                lokasi_master=lokasi_master,
                lokasi=lokasi_name,
                sungai=sungai,
                kota_kabupaten=kota_kabupaten,
                sampling=sampling,
                periode=periode,
                pi=pi,
                keterangan=keterangan,
                ll=ll,
                doc_path=doc_filename,
                foto_path=foto_filename,
                lembaga=lembaga,
                kelas_baku_mutu=kelas_baku_mutu,
                username=current_user.username
            )
            
            # Handle parameter details
            parameters_json = request.form.get('parameters_json', '{}')
            try:
                parameters = json.loads(parameters_json)
                for param_name, nilai in parameters.items():
                    if nilai:  # Only save non-empty values
                        # Find parameter definition
                        param_def = next((p for p in PARAMETER_LIST if p['name'] == param_name), None)
                        if param_def:
                            ParameterDetail.create(
                                hasil_uji=hu,
                                parameter_name=param_name,
                                satuan=param_def.get('satuan'),
                                nilai=str(nilai)
                            )
            except json.JSONDecodeError:
                pass  # Skip parameters if JSON invalid
            
            flash(f'Data Kualitas Air "{lokasi_name}" dengan {len(parameters)} parameter berhasil ditambahkan!')
            return redirect(f'/pos/ka?tahun={tahun}')
        except Exception as e:
            flash(f'Error: {str(e)}')
            import traceback
            traceback.print_exc()
            return redirect(request.url)
    
    # GET request - render form
    ctx = {
    'tahun': tahun,
    'form': form,
    'parameter_list': PARAMETER_LIST,
    'parameter_values': [],
    'edit_mode': False,
    'record': None,
    'lokasi_master_id': None,
    'lokasi_nama': '',
}
    return render_template('pos/add_ka_new.html', ctx=ctx)

@bp.route('/ka/edit/<int:record_id>', methods=['GET', 'POST'])
@login_required
def edit_data_ka(record_id):
    try:
        hu = HasilUjiKualitasAir.get_by_id(record_id)
    except HasilUjiKualitasAir.DoesNotExist:
        flash('Data tidak ditemukan!')
        return redirect('/pos/ka')
    
    form = HasilUjiKAForm()
    tahun = int(request.args.get('tahun', hu.sampling.year))
    
    if form.validate_on_submit():
        try:
            # Get form data
            lokasi_id = request.form.get('lokasi_id')
            sungai = form.sungai.data.strip() if form.sungai.data else None
            kota_kabupaten = form.kota_kabupaten.data.strip() if form.kota_kabupaten.data else None
            pi = form.pi.data
            keterangan = form.keterangan.data.strip() if form.keterangan.data else None
            periode = int(form.periode.data)
            lembaga = form.lembaga.data.strip() if form.lembaga.data else None
            ll = form.ll.data.strip() if form.ll.data else None
            kelas_baku_mutu = int(form.kelas_baku_mutu.data) if form.kelas_baku_mutu.data else 2
            
            # Get lokasi_master if lokasi_id provided
            lokasi_master = None
            lokasi_name = None
            if lokasi_id and lokasi_id.isdigit():
                lokasi_master = LokasiMaster.get_by_id(int(lokasi_id))
                lokasi_name = lokasi_master.nama_lokasi
                # Override dengan data dari master jika tidak diisi manual
                if not sungai: sungai = lokasi_master.sungai
                if not kota_kabupaten: kota_kabupaten = lokasi_master.kota_kabupaten
                if not ll: ll = lokasi_master.koordinat
            else:
                # Fallback untuk manual input
                lokasi_name = request.form.get('lokasi', hu.lokasi)
            
            # Use sampling date from form (required)
            sampling = form.sampling.data
            
            # Update record
            hu.lokasi_master = lokasi_master
            hu.lokasi = lokasi_name
            hu.sungai = sungai
            hu.kota_kabupaten = kota_kabupaten
            hu.sampling = sampling
            hu.periode = periode
            hu.pi = pi
            hu.keterangan = keterangan
            hu.ll = ll
            hu.lembaga = lembaga
            hu.kelas_baku_mutu = kelas_baku_mutu
            hu.mdate = datetime.datetime.now()
            
            # Handle file uploads if provided (optional)
            if 'fname' in request.files and request.files['fname'].filename != '':
                file = request.files['fname']
                doc_filename = secure_filename(file.filename or '')
                rel_dir = f"{current_app.config['KUALITAS_AIR_FOLDER']}/{sampling.strftime('_%Y/_%m')}"
                full_dir = os.path.join('app', rel_dir)
                os.makedirs(full_dir, exist_ok=True)
                full_file_path = os.path.join(full_dir, doc_filename)
                file.save(full_file_path)
                hu.doc_path = doc_filename
            
            if 'foto' in request.files and request.files['foto'].filename != '':
                file = request.files['foto']
                foto_filename = secure_filename(file.filename or '')
                rel_dir = f"{current_app.config['KUALITAS_AIR_FOLDER']}/{sampling.strftime('_%Y/_%m')}"
                full_dir = os.path.join('app', rel_dir)
                os.makedirs(full_dir, exist_ok=True)
                full_file_path = os.path.join(full_dir, foto_filename)
                file.save(full_file_path)
                hu.foto_path = foto_filename
            
            hu.save()
            
            # Handle parameter details - delete old ones and create new ones
            ParameterDetail.delete().where(ParameterDetail.hasil_uji == hu).execute()
            
            parameters_json = request.form.get('parameters_json', '{}')
            try:
                parameters = json.loads(parameters_json)
                for param_name, nilai in parameters.items():
                    if nilai:  # Only save non-empty values
                        # Find parameter definition
                        param_def = next((p for p in PARAMETER_LIST if p['name'] == param_name), None)
                        if param_def:
                            ParameterDetail.create(
                                hasil_uji=hu,
                                parameter_name=param_name,
                                satuan=param_def.get('satuan'),
                                nilai=str(nilai)
                            )
            except json.JSONDecodeError:
                pass  # Skip parameters if JSON invalid
            
            flash(f'Data Kualitas Air "{lokasi_name}" berhasil diperbarui!')
            return redirect(f'/pos/ka?tahun={tahun}')
        except Exception as e:
            flash(f'Error: {str(e)}')
            import traceback
            traceback.print_exc()
            return redirect(request.url)
    
    # Pre-populate form with existing data
    if request.method == 'GET':
        form.lokasi.data = hu.lokasi
        form.sungai.data = hu.sungai
        form.kota_kabupaten.data = hu.kota_kabupaten
        form.sampling.data = hu.sampling
        form.periode.data = str(hu.periode)
        form.ll.data = hu.ll
        form.pi.data = hu.pi
        form.keterangan.data = hu.keterangan
        form.lembaga.data = hu.lembaga
        form.kelas_baku_mutu.data = str(hu.kelas_baku_mutu) if hu.kelas_baku_mutu else '2'
        hu.lokasi_master_id = hu.lokasi_master.id if hu.lokasi_master else None
    
    # Get parameter values for display - convert to dicts for JSON serialization
    parameter_details = list(
        ParameterDetail.select().where(ParameterDetail.hasil_uji == hu)
    )
    parameter_values = [
        {
            'parameter_name': pd.parameter_name,
            'satuan': pd.satuan,
            'nilai': pd.nilai
        }
        for pd in parameter_details
    ]
    
    ctx = {
    'tahun': tahun,
    'form': form,
    'parameter_list': PARAMETER_LIST,
    'parameter_values': parameter_values,
    'edit_mode': True,
    'record': hu,
    'lokasi_master_id': hu.lokasi_master.id if hu.lokasi_master else None,
    'lokasi_nama': hu.lokasi or '',
}
    return render_template('pos/add_ka_new.html', ctx=ctx)

@bp.route('/ka')
@login_required
def data_ka():
    tahun = request.args.get('tahun', datetime.date.today().year, type=int)
    
    # Get all data untuk tahun ini, ordered by lokasi and periode
    all_data = (HasilUjiKualitasAir.select()
                .where(HasilUjiKualitasAir.sampling.year == tahun)
                .order_by(HasilUjiKualitasAir.lokasi, HasilUjiKualitasAir.periode))
    
    # Detect which periodes have data and get their actual months
    periode_months = {}  # {periode: 'Bulan'}
    for hu in all_data:
        if hu.periode and 1 <= hu.periode <= 3 and hu.periode not in periode_months:
            # Get month name from sampling date
            bulan_name = hu.sampling.strftime('%B')
            periode_months[hu.periode] = bulan_name
    
    # Sort periode that have data
    periode_with_data = sorted(periode_months.keys())
    
    # Reorganize data by lokasi with nested periode structure
    lokasi_data = {}
    for hu in all_data:
        if hu.lokasi not in lokasi_data:
            lokasi_data[hu.lokasi] = {
                'lokasi': hu.lokasi,
                'sungai': hu.sungai,
                'kota_kabupaten': hu.kota_kabupaten,
                'periode': {}  # use dict instead of list
            }
        
        if hu.periode and 1 <= hu.periode <= 3:
            lokasi_data[hu.lokasi]['periode'][hu.periode] = {
                'bulan': periode_months.get(hu.periode, ''),
                'pi': hu.pi,
                'keterangan': hu.keterangan,
                'status': hu.status_hasil_uji,
                'color': hu.color_status,
                'id': hu.id
            }
    
    # Get ALL lokasi dari LokasiMaster untuk show empty rows juga
    all_lokasi_master = LokasiMaster.select().order_by(LokasiMaster.nama_lokasi)
    
    # Merge: ensure all lokasi dari master appear in table, even if no data
    for lokasi_master in all_lokasi_master:
        if lokasi_master.nama_lokasi not in lokasi_data:
            lokasi_data[lokasi_master.nama_lokasi] = {
                'lokasi': lokasi_master.nama_lokasi,
                'sungai': lokasi_master.sungai,
                'kota_kabupaten': lokasi_master.kota_kabupaten,
                'periode': {}  # Empty periode dict = no data for this year
            }
    
    # Convert dict to list, sorted by lokasi
    data_ka_nested = sorted(lokasi_data.values(), key=lambda x: x['lokasi'])
    
    ctx = {
        'tahun': tahun,
        'now': datetime.date.today(),
        'data_ka': data_ka_nested,
        'periode_with_data': periode_with_data,
        'periode_months': periode_months
    }
    return render_template('pos/data_ka.html', ctx=ctx)

@bp.route('/ka/detail/<lokasi>')
@login_required
def detail_ka(lokasi):
    tahun = request.args.get('tahun', datetime.date.today().year, type=int)

    tahun_list = [tahun, tahun - 1, tahun - 2]
    chart_data_by_year = {}
    param_data_by_year = {}  # ← baru
    available_years = []

    for y in tahun_list:
        rows = list(HasilUjiKualitasAir.select()
                    .where(HasilUjiKualitasAir.lokasi == lokasi,
                           HasilUjiKualitasAir.sampling.year == y)
                    .order_by(HasilUjiKualitasAir.sampling))
        if not rows:
            continue

        available_years.append(y)
        labels = [f"Periode {r.periode} ({r.sampling.strftime('%b %Y')})" for r in rows]

        chart_data_by_year[y] = {
            'labels': labels,
            'data': [r.pi for r in rows if r.pi is not None]
        }

        # Kumpulkan data parameter per tahun
        param_data_by_year[y] = {'labels': labels}
        for r in rows:
            for pd in ParameterDetail.select().where(ParameterDetail.hasil_uji == r):
                if pd.parameter_name not in param_data_by_year[y]:
                    param_data_by_year[y][pd.parameter_name] = []
                try:
                    val = float(str(pd.nilai).replace('<','').replace('>','').strip())
                except:
                    val = None
                param_data_by_year[y][pd.parameter_name].append(val)

    # Kumpulkan semua nama parameter yang tersedia
    parameter_names = sorted(set(
        k for y_data in param_data_by_year.values()
        for k in y_data.keys()
        if k != 'labels'
    ))

    # Data tahun aktif untuk tabel
    all_data = list(HasilUjiKualitasAir.select()
                    .where(HasilUjiKualitasAir.lokasi == lokasi,
                           HasilUjiKualitasAir.sampling.year == tahun)
                    .order_by(HasilUjiKualitasAir.sampling))

    table_data = [
        {'sampling': hu.sampling, 'pi': hu.pi,
         'status': hu.status_hasil_uji, 'keterangan': hu.keterangan}
        for hu in all_data if hu.pi is not None
    ]

    ctx = {
        'lokasi': lokasi,
        'tahun': tahun,
        'available_years': available_years,
        'chart_data_by_year': chart_data_by_year,
        'param_data_by_year': param_data_by_year,   # ← baru
        'parameter_names': parameter_names,           # ← baru
        'table_data': table_data,
        'all_data': all_data,
    }
    return render_template('pka/detail.html', ctx=ctx)

@bp.route('/luwes')
@login_required
def pos_luwes():
    ctx = {
        'poses': LuwesPos.select().order_by(LuwesPos.tipe, LuwesPos.nama)
    }
    return render_template('pos/luwes.html', ctx=ctx)

@bp.route('/luwes/migrasi')
@login_required
def migrasi_luwes():
    lines = []
    with open('migration.txt', 'r', encoding='utf-8') as f:
        lines = f.readlines()
    reading_time = lines[0].strip()
    # Skip header lines and process data
    data_lines = []
    for line in lines[2:]:  # Skip header and separator
        line = line.strip()
        if line and '|' in line:
            parts = [part.strip() for part in line.split('|')]
            if len(parts) >= 3:
                # Convert checkmarks to HTML entities for better compatibility
                parts = [part.replace('✓', '&#10004;') for part in parts]
                data_lines.append(parts)
    
    return render_template('pos/migrasiluwes.html', data_lines=data_lines, reading_time=reading_time)


@bp.route('/manual/kinerja')
@login_required
def kinerja_manual():
    sampling = request.args.get('s', None) == None and \
        datetime.datetime.now() or \
            datetime.datetime.strptime(request.args.get('s'), '%Y-%m-%d')
    
    (_s, s, s_) = get_sampling(sampling.strftime('%Y-%m-1'))
    _s = s - datetime.timedelta(days=2)
    if s.strftime('%Y%m') >= datetime.date.today().strftime('%Y%m'):
        s_ = None
    else:
        s_ = (s + datetime.timedelta(days=32)).replace(day=1)
        
    num_hari = (s_ and (s_ - s) or (datetime.datetime.now() - s)).days
    all_pos = (Pos.select().where(Pos.tipe.in_(('1','2','3')))
               .order_by(Pos.tipe, Pos.nama))
    mdaily = (ManualDaily.select()
              .where(ManualDaily.sampling.year==s.year, 
                     ManualDaily.sampling.month==s.month)
              .order_by(ManualDaily.pos, ManualDaily.sampling))
    pchs = []
    pdas = []
    for p in all_pos:
        if p.tipe == '1':
            pchs.append(p)
            num_data = len([m for m in mdaily if m.pos_id==p.id])
            banyak_data = num_hari
        elif p.tipe == '2':
            pdas.append(p)
            num_data = sum([len(json.loads(m.tma)) / 2 for m in mdaily if m.pos_id==p.id])
            banyak_data = num_hari * 3
        delta_entry = sum([(m.cdate - (datetime.datetime.combine(m.sampling, datetime.time(7, 0)) + datetime.timedelta(days=1)).replace(hour=7, minute=0, second=0)).total_seconds() for m in mdaily if m.pos_id==p.id])
        p.delta_entry = datetime.timedelta(seconds=delta_entry)
        p.diterima = num_data
        p.seharusnya = banyak_data
        p.persen = ((num_data / banyak_data) * 100) if banyak_data else 0
    
    pch_diterima = sum([p.diterima for p in pchs])
    pch_seharusnya = sum([p.seharusnya for p in pchs])
    pda_diterima = sum([p.diterima for p in pdas])
    pda_seharusnya = sum([p.seharusnya for p in pdas])
        
    ctx = {
        's': s,
        '_s': _s,
        's_': s_,
        'all_pos': all_pos,
        'pchs': {'diterima': pch_diterima, 'seharusnya': pch_seharusnya, 'banyak_pos': len(pchs)},
        'pdas': {'diterima': pda_diterima, 'seharusnya': pda_seharusnya, 'banyak_pos': len(pdas)},
        'num_hari': num_hari
    }
    return render_template('pos/manual/kinerja.html', ctx=ctx)


@bp.route('/manual')
@login_required
def manual():
    formhujan = CurahHujanForm()
    if current_user.is_anonymous:
        abort(404)
    if not current_user.is_admin:
        abort(404)
    (_s, s, s_) = get_sampling(request.args.get('s', None))
    data_pch = ManualDaily.select().where(ManualDaily.sampling==_s.strftime('%Y-%m-%d'))
    data_other = ManualDaily.select().where(ManualDaily.sampling==s.strftime('%Y-%m-%d'))

    data_manual_pch = dict([(p.pos.id, p.ch) for p in data_pch if p.pos.tipe in ('1', '3')])
    data_manual_pda = dict([(p.pos.id, p._tma) for p in data_other if p.pos.tipe=='2'])

    data = Pos.select().order_by(Pos.tipe, Pos.nama)
    pch = [p for p in data if p.tipe in ('1', '3')]

    for p in pch:
        if p.petugas_set:
            p.petugas = p.petugas_set[0].nama
        else:
            p.petugas = None
        if p.id in data_manual_pch:
            p.ch = data_manual_pch[p.id]
        else:
            p.ch = ''
    pda = [p for p in data if p.tipe=='2']
    for p in pda:
        if p.petugas_set:
            p.petugas = p.petugas_set[0].nama
        else:
            p.petugas = None
        if p.id in data_manual_pda:
            p.tma = data_manual_pda[p.id]
        else:
            p.tma = None
    ctx = {
        '_sampling': _s,
        'sampling': s,
        'sampling_': s_,
        'pch': pch,
        'pda': pda,
        'formhujan': formhujan
    }
    return render_template('pos/manual/index.html', ctx=ctx)


@bp.route('/<int:pos_id>/manual/<int:tahun>/<int:bulan>')
@login_required
def show_manual(pos_id, tahun=datetime.date.today().year, bulan=datetime.date.today().month):
    try:
        pos = Pos.get(pos_id)
    except DoesNotExist:
        return abort(404)
    samp = "{}-{}-1".format(tahun, bulan)
    (_s, s, s_) = get_sampling(samp)
    _s = s - datetime.timedelta(days=2)
    if s.strftime('%Y%m') >= datetime.date.today().strftime('%Y%m'):
        s_ = None
    else:
        s_ = (s + datetime.timedelta(days=32)).replace(day=1)
    mdaily = ManualDaily.select().where(ManualDaily.pos_id==pos_id, 
                                        ManualDaily.sampling.year==s.year,
                                        ManualDaily.sampling.month==s.month).order_by(ManualDaily.sampling)
    delta_time = datetime.timedelta()
    for m in mdaily:
        m.delta_entry = m.cdate - (datetime.datetime.combine(m.sampling, datetime.time(7, 0)) + datetime.timedelta(days=1)).replace(hour=7, minute=0, second=0)
        delta_time += m.delta_entry
    
    by_petugas = [i for i in mdaily if i.is_by_petugas]
    by_other = [i for i in mdaily if not i.is_by_petugas]

    entry_count = (ManualDaily
                   .select(ManualDaily.cdate.year, 
                           ManualDaily.cdate.month, 
                           ManualDaily.cdate.day, fn.Count(ManualDaily.cdate))
                   .where(ManualDaily.pos_id==pos.id, 
                          ManualDaily.sampling.year==s.year,
                          ManualDaily.sampling.month==s.month)
                   .group_by(ManualDaily.cdate.year,
                             ManualDaily.cdate.month,
                             ManualDaily.cdate.day)
                   .order_by(ManualDaily.cdate.day).tuples())
    ec = [(datetime.date(int(a), int(b), int(c)), int(d)) for a, b, c, d in entry_count]
        
    ctx = {
        'pos': pos,
        'mdaily': mdaily,
        'num_hari': (s_ and (s_ - s) or (datetime.datetime.now() - s)).days,
        'entry_count': ec,
        'delta_time': delta_time,
        'by_petugas': len(mdaily) != 0 and (len(by_petugas) / len(mdaily) * 100, datetime.timedelta(seconds=sum([i.delta_entry.total_seconds() for i in mdaily if i.is_by_petugas]))) or 0,
        'by_other': len(mdaily) != 0 and (len(by_other) / len(mdaily) * 100,  datetime.timedelta(seconds=sum([i.delta_entry.total_seconds() for i in mdaily if not i.is_by_petugas]))) or 0,
        '_s': _s,
        's': s,
        's_': s_
    }
    return render_template('pos/manual/show.html', ctx=ctx)

@bp.route('/<int:id>/manual', methods=['POST'])
@login_required
def upsert_manual(id):
    pos = Pos.get(id)
    if pos.tipe in ('1', '3'):
        form = CurahHujanForm()
        if form.validate_on_submit():
            ret = {'ok': True, 'ch': form.ch.data, 
                'sampling': form.sampling.data, 
                'pos': pos.id,
                'username': current_user.username}
            md = ManualDaily.select().where(
                ManualDaily.pos==pos, 
                ManualDaily.sampling==form.sampling.data).first()
            if md:
                md.ch = form.ch.data
                md.save()
            else:
                md = ManualDaily.create(**ret)
        else:
            print(form.errors)
            ret = {'ok': False, 'error': form.errors}
    elif pos.tipe == '2':
        form = TmaForm()
        if form.validate_on_submit():
            md = ManualDaily.select().where(
                ManualDaily.pos==pos, 
                ManualDaily.sampling==form.sampling.data).first()
            if md:
                tma = json.loads(md.tma)
                if 'cdate_'+str(form.jam.data) in tma:
                    tma.update({str(form.jam.data): form.tma.data})
                else:
                    tma.update({str(form.jam.data): form.tma.data, 
                                'cdate_'+str(form.jam.data): datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
                md.tma = json.dumps(tma)
                md.save()
                ret = {'ok': True, 'tma': tma,
                    'sampling': form.sampling.data,
                    'pos': pos.id,
                    'username': current_user.username}
            else:
                tma = json.dumps({str(form.jam.data): form.tma.data, 
                                  'cdate_'+str(form.jam.data): datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
                ret = {'ok': True, 'tma': tma,
                    'sampling': form.sampling.data,
                    'pos': pos.id,
                    'username': current_user.username}
                md = ManualDaily.create(**ret)
        else:
            print(form.errors)
            ret = {'ok': False, 'error': form.errors}
    if form.fetch.data == True:
        return jsonify(ret)
    else:
        return redirect('/')

@bp.route('/debit')
@login_required
def lengkung_debit():
    '''Hitung DEBIT
    Q = c (H + a)^b
    Q = Debit
    H = TMA
    '''
    ctx = {
        'poses': LengkungDebit.select()
    }
    return render_template('pos/lengkung_debit.html', ctx=ctx)

@bp.route('/')
@login_required
def index():
    pm = dict([(p.pos.id, p) for p in PosMap.select()])
    op = dict([(p.pos.id, p) for p in OPos.select() if p.pos_id != None])
    poses = Pos.select().order_by(Pos.tipe, Pos.nama, Pos.elevasi.desc())
    for p in poses:
        if p.id in pm:
            p.source = pm[p.id].nama
        if p.id in op:
            p.vendor = op[p.id].source
    return render_template('pos/index.html', poses=poses)

@bp.route('/ka/export/<int:record_id>')
@login_required
def export_ka_pdf(record_id):
    try:
        hu = HasilUjiKualitasAir.get_by_id(record_id)
    except HasilUjiKualitasAir.DoesNotExist:
        abort(404)

    # Ambil parameter details
    params = list(ParameterDetail.select().where(ParameterDetail.hasil_uji == hu))

    # Hitung kolom tambahan untuk tabel PDF
    # Baku mutu kelas 2 (sesuai Kepmen LH 115/2003)
    BAKU_MUTU_KELAS2 = {
        'Temperatur':                     {'max': 3,    'satuan': '°C'},
        'Padatan Terlarut Total (TDS)':   {'max': 1000,  'satuan': 'mg/L'},
        'Padatan Tersuspensi Total (TSS)':{'max': 50,    'satuan': 'mg/L'},
        'Derajat Keasaman (pH)':          {'max': 7.5,     'satuan': '-'},
        'Kebutuhan Oksigen Biokimiawi (BOD)': {'max': 3, 'satuan': 'mg/L'},
        'Kebutuhan Oksigen Kimiawi (COD)':    {'max': 25,'satuan': 'mg/L'},
        'Oksigen Terlarut (DO)':          {'max': 4,     'satuan': 'mg/L'},
        'Nitrat (sebagai N)':             {'max': 10,    'satuan': 'mg/L'},
        'Nitrit (sebagai N)':             {'max': 0.06,  'satuan': 'mg/L'},
        'Total Fosfat (Sebagai P)':       {'max': 0.2,   'satuan': 'mg/L'},
        'Kadmium (Cd) Terlarut':          {'max': 0.01,  'satuan': 'mg/L'},
        'Seng (Zn) Terlarut':             {'max': 0.05,  'satuan': 'mg/L'},
        'Tembaga (Cu) Terlarut':          {'max': 0.02,  'satuan': 'mg/L'},
        'Deterjen Total':                 {'max': 0.2,   'satuan': 'mg/L'},
        'Fecal Coliform':                 {'max': 1000,  'satuan': 'MPN/100mL'},
        'Total Coliform':                 {'max': 5000,  'satuan': 'MPN/100mL'},
        'Kekeruhan':                      {'max': '',    'satuan': 'NTU'},
    }

    import math
    param_rows = []
    ci_baru_vals = []  # ← ini yang dipakai untuk max, avg, PI

    for pd in params:
        bm = BAKU_MUTU_KELAS2.get(pd.parameter_name, {})
        bm_max = bm.get('max')
        try:
            nilai_float = float(str(pd.nilai).replace('<', '').replace('>', '').strip())
        except (ValueError, TypeError):
            nilai_float = None

        ci_ci_max = None
        ci_baru = None  # (Cᵢ/Lᵢⱼ)baru

        if nilai_float is not None and bm_max:
            ci_ci_max = round(nilai_float / bm_max, 4)
            # Rumus (Cᵢ/Lᵢⱼ)baru: IF(ci > 1, 1 + 5*LOG10(ci), ci)
            if ci_ci_max > 1:
                ci_baru = round(1 + 5 * math.log10(ci_ci_max), 4)
            else:
                ci_baru = ci_ci_max
            ci_baru_vals.append(ci_baru)

        param_rows.append({
            'name': pd.parameter_name,
            'satuan': pd.satuan,
            'nilai': pd.nilai,
            'bm_max': bm_max,
            'ci_ci_max': ci_ci_max,
            'ci_baru': ci_baru,   # ← ganti log_val jadi ci_baru
        })

    # Hitung dari kolom (Cᵢ/Lᵢⱼ)baru
    if ci_baru_vals:
        max_val = max(ci_baru_vals)
        avg_val = sum(ci_baru_vals) / len(ci_baru_vals)
        pi_hitung = round(math.sqrt((avg_val**2 + max_val**2) / 2), 3)
    else:
        max_val = avg_val = pi_hitung = None

    # Status berdasarkan pi_hitung (bukan hu.pi yang manual)
    def get_status(pi):
        if pi is None: return '-'
        if pi <= 1:  return 'memenuhi baku mutu'
        if pi <= 5:  return 'cemar ringan'
        if pi <= 10: return 'cemar sedang'
        return 'cemar berat'

    status_hitung = get_status(pi_hitung)

    html_string = render_template('pos/export_ka_pdf.html',
        hu=hu,
        params=param_rows,
        max_val=round(max_val, 4) if max_val is not None else '-',
        avg_val=round(avg_val, 4) if avg_val is not None else '-',
        pi_hitung=pi_hitung if pi_hitung is not None else '-',
        status=status_hitung,  # ← pakai hasil hitung, bukan hu.status_hasil_uji
        now=datetime.datetime.now(),
    )

    pdf_bytes = WeasyprintHTML(string=html_string, base_url=request.host_url).write_pdf()

    filename = f"KualitasAir_{hu.lokasi}_{hu.sampling}_Periode{hu.periode}.pdf"
    return current_app.response_class(
        pdf_bytes,
        mimetype='application/pdf',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'}
    )