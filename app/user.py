from flask import Blueprint, render_template, flash, redirect, url_for, abort, request
from flask_login import login_required, current_user
from flask_wtf import FlaskForm

from app.models import User, Pos
from app.forms import UserForm
from app import admin_required

bp = Blueprint('user', __name__, url_prefix='/user')

@bp.route('/<username>/update', methods=['POST'])
@login_required
@admin_required
def update(username):
    if not FlaskForm().validate_on_submit():
        abort(400)
    password = request.form.get('password', '')
    if len(password) < 6:
        flash('Password minimal 6 karakter')
        return redirect(url_for('user.index'))
    try:
        u = User.get(User.username == username)
    except User.DoesNotExist:
        abort(404)
    u.set_password(password)  # sudah hash + save
    flash('Password {} diganti'.format(u.username))
    return redirect(url_for('user.index'))

@bp.route('/add', methods=['POST'])
def add():
    form = UserForm()
    form.pos.choices = [('', 'Kantor')] + [(p.id, p.nama) for p in Pos.select().order_by(Pos.nama)]
    if form.validate_on_submit():
        if form.data.get('pos') == '':
            new_data = form.data
            new_data.update({'pos': None})
        else:
            new_data = form.data
        print(form.data)
        new_user = User.create(**new_data)
        new_user.set_password(new_data.get('password'))
        new_user.save()
        flash('Sukses')
        return redirect(url_for('user.index'))
    else:
        print(form.data)
        return render_template('user/add.html', form=form)


@bp.route('/')
@login_required
@admin_required
def index():
    users = User.select().order_by(User.username)
    users_kantor = [u for u in users if u.pos_id is None]
    users_petugas_pch = [u for u in users if u.pos_id is not None and (u.pos.tipe == '1' or u.pos.tipe == '3')]
    users_petugas_pda = [u for u in users if u.pos_id is not None and u.pos.tipe == '2']
    userform = UserForm()
    userform.pos.choices = [('', 'Kantor')] + [(p.id, p.nama) for p in Pos.select().order_by(Pos.nama)]
    
    return render_template('user/index.html', 
                           users_kantor=users_kantor, 
                           users_petugas_pch=users_petugas_pch,
                           users_petugas_pda=users_petugas_pda,
                           form=userform)


@bp.route('/<int:id>/toggle', methods=['POST'])
@login_required
@admin_required
def toggle(id):
    if not FlaskForm().validate_on_submit():
        abort(400)
    try:
        u = User.get(User.id == id)
    except User.DoesNotExist:
        abort(404)
    if u.id == current_user.id:
        flash('Tidak bisa menonaktifkan akun sendiri')
        return redirect(url_for('user.index'))
    if u.active and u.pos_id is None:
        sisa = User.select().where(User.pos.is_null(True), User.active == True, User.id != u.id).count()
        if sisa == 0:
            flash('Minimal harus ada satu admin kantor aktif')
            return redirect(url_for('user.index'))
    u.active = not u.active
    u.save()
    flash('{} sekarang {}'.format(u.username, 'aktif' if u.active else 'nonaktif'))
    return redirect(url_for('user.index'))