from flask import Blueprint, render_template, jsonify, request
from flask import abort, redirect, flash, current_app
from flask_login import current_user, login_required
from werkzeug.utils import secure_filename
import os
import json
import datetime
from peewee import DoesNotExist, fn

from app.models import Pos, ManualDaily, PosMap, OPos, LengkungDebit, LuwesPos, HasilUjiKualitasAir
from app import get_sampling
from app.forms import CurahHujanForm, TmaForm, HasilUjiKAForm
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
        hu = HasilUjiKualitasAir.get(HasilUjiKualitasAir.id==id)
    except DoesNotExist:
        return abort(404)
    try:
        path = 'static/ka/' + hu.sampling.strftime('_%Y/_%m/') + hu.doc_path
        if hu.doc_path and os.path.exists('app/' + path): os.remove('app/' + path)
        hu.delete_instance()
        flash('Data hasil uji kualitas air berhasil dihapus.', 'success')
    except Exception as e:
        flash('Gagal menghapus data hasil uji kualitas air: {}'.format(str(e)), 'danger')
    return redirect('/pos/ka')

@bp.route('/ka/add', methods=['GET', 'POST'])
@login_required
def add_data_ka():
    pos_id = request.args.get('pid')
    form = HasilUjiKAForm()
    try:
        pos = Pos.get(int(pos_id))
    except DoesNotExist:
        return abort(404)
    
    try:
        s = request.args.get('s')
        sampling = datetime.date(int(s.split('-')[0]), int(s.split('-')[1]), 1)
    except:
        sampling = datetime.date.today()
    if form.validate_on_submit():
        if 'fname' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['fname']
        if file.filename == '':
            flash('File Hasil Uji Lab belum diisi')
            return redirect(request.url)
        fname = secure_filename(file.filename or '')
        # Create directory path - consistent for both makedirs and file.save
        rel_dir = f"{current_app.config['KUALITAS_AIR_FOLDER']}/{sampling.strftime('_%Y/_%m')}"
        full_dir = os.path.join('app', rel_dir)
        os.makedirs(full_dir, exist_ok=True)
        full_file_path = os.path.join(full_dir, fname)
        file.save(full_file_path)
        flash(f'File {fname} uploaded successfully!')
        ret = {'pos': form.pos.data, 
               'sampling': form.sampling.data, 
               'll': form.ll.data, 
               'doc_path': fname, 
               'lembaga': form.lembaga.data,
               'username': current_user.username,
               'status_hasil_uji': form.status_hasil_uji.data
               }
        hu = HasilUjiKualitasAir.create(**ret)
        return redirect('/pos/ka')
    else:
        form.pos.data = pos_id
        form.sampling.data = request.args.get('s')
        form.ll.data = pos.ll
    ctx = {
        'pos': pos,
        'sampling': sampling,
        'form': form
    }
    return render_template('pos/add_ka.html', ctx=ctx)

@bp.route('/ka')
@login_required
def data_ka():
    (_sampling, sampling, sampling_) = get_sampling(request.args.get('s', None))
    poska = Pos.select().where(Pos.tipe=='4').order_by(Pos.sungai)
    if sampling.month < 7:
        sampling = sampling.replace(month=1)
        _sampling = _sampling.replace(month=7, year=sampling.year - 1)
        if sampling_:
            sampling_ = sampling_.replace(month=7)
    else:
        sampling = sampling.replace(month=7)
        _sampling = _sampling.replace(month=1)
        if sampling_:
            sampling_ = sampling_.replace(month=1, year=sampling.year + 1)
    sungai = set([p.sungai for p in poska])
    months = [sampling.month + m for m in range(6)]
    huka = (HasilUjiKualitasAir.select()
            .where(HasilUjiKualitasAir.sampling.year==sampling.year,
                   HasilUjiKualitasAir.sampling.month.in_(months))
            .order_by(HasilUjiKualitasAir.sampling))
    hasil_uji = {}
    for hu in huka:
        hasil_uji.update({'{}_{}'.format(hu.pos_id, hu.sampling.month):  hu})

    out = {}
    for s in sungai:
        out.update({s: [p for p in poska if p.sungai==s]})
    ctx = {
        '_sampling': _sampling,
        'sampling': sampling,
        'sampling_': sampling_,
        'poses': poska,
        'sungai': out,
        'hasil_uji': hasil_uji
    }
    return render_template('pos/data_ka.html', ctx=ctx)

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